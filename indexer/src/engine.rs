use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use ignore::WalkBuilder;
use rayon::prelude::*;
use tracing::{debug, trace, warn};

use crate::chunk_store::ChunkStore;
use crate::config::IndexerConfig;
use crate::extractors::{self, ExtractedSymbol};
use crate::models::{
    BranchHead, ChunkMapping, ContentBlob, FilePointer, IndexArtifacts, IndexReport,
    ReferenceRecord, SymbolRecord,
};
use crate::utils;

const MIN_CHUNK_SIZE: u32 = 64 * 1024;
const AVG_CHUNK_SIZE: u32 = 256 * 1024;
const MAX_CHUNK_SIZE: u32 = 1024 * 1024;

pub struct Indexer {
    config: IndexerConfig,
}

impl Indexer {
    pub fn new(config: IndexerConfig) -> Self {
        Self { config }
    }

    pub fn run(&self) -> Result<IndexArtifacts> {
        let walker = WalkBuilder::new(&self.config.repo_path)
            .git_ignore(true)
            .git_exclude(true)
            .hidden(false)
            .ignore(true)
            .build();

        let mut work_items = Vec::new();

        for entry in walker {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    warn!(error = %err, "failed to read directory entry");
                    continue;
                }
            };

            if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                trace!(path = %entry.path().display(), "skipping non-file entry");
                continue;
            }

            let absolute_path = entry.path().to_path_buf();
            let relative_path = match utils::ensure_relative(&absolute_path, &self.config.repo_path)
            {
                Ok(path) => path,
                Err(err) => {
                    warn!(
                        error = %err,
                        path = %absolute_path.display(),
                        "skipping file outside repo root"
                    );
                    continue;
                }
            };

            if should_skip(&relative_path) {
                trace!(path = %relative_path.display(), "skipping filtered file");
                continue;
            }

            work_items.push(FileEntry {
                absolute: absolute_path,
                relative: relative_path,
            });
        }

        let chunk_store = Arc::new(Mutex::new(ChunkStore::new()?));
        let content_blobs = Arc::new(Mutex::new(Vec::new()));
        let file_pointers = Arc::new(Mutex::new(Vec::new()));
        let symbol_records = Arc::new(Mutex::new(Vec::new()));
        let reference_records = Arc::new(Mutex::new(Vec::new()));
        let chunk_mappings = Arc::new(Mutex::new(Vec::new()));
        let seen_hashes = Arc::new(Mutex::new(HashSet::new()));

        let config = self.config.clone();

        work_items
            .par_iter()
            .for_each(|entry| match process_file(&config, entry) {
                Ok(file_artifacts) => {
                    let FileArtifacts {
                        content_blob,
                        file_pointer,
                        symbol_records: file_symbols,
                        reference_records: file_references,
                        chunk_mappings: file_chunk_mappings,
                        chunk_writes,
                    } = file_artifacts;

                    let content_hash = file_pointer.content_hash.clone();

                    file_pointers
                        .lock()
                        .expect("file pointers mutex poisoned")
                        .push(file_pointer);

                    symbol_records
                        .lock()
                        .expect("symbol records mutex poisoned")
                        .extend(file_symbols);

                    reference_records
                        .lock()
                        .expect("reference records mutex poisoned")
                        .extend(file_references);

                    let is_new_content = {
                        let mut seen = seen_hashes.lock().expect("seen hashes mutex poisoned");
                        seen.insert(content_hash.clone())
                    };

                    if is_new_content {
                        content_blobs
                            .lock()
                            .expect("content blobs mutex poisoned")
                            .push(content_blob);

                        chunk_mappings
                            .lock()
                            .expect("chunk mappings mutex poisoned")
                            .extend(file_chunk_mappings);

                        let mut store = chunk_store.lock().expect("chunk store mutex poisoned");
                        for chunk in chunk_writes {
                            if let Err(err) = store.insert(chunk.hash, chunk.text_content) {
                                warn!(%content_hash, error = %err, "failed to insert chunk");
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "failed to process file");
                }
            });

        let chunk_store = Arc::try_unwrap(chunk_store)
            .expect("chunk store still has outstanding references")
            .into_inner()
            .expect("chunk store mutex poisoned");
        let content_blobs = Arc::try_unwrap(content_blobs)
            .expect("content blobs still have outstanding references")
            .into_inner()
            .expect("content blobs mutex poisoned");
        let file_pointers = Arc::try_unwrap(file_pointers)
            .expect("file pointers still have outstanding references")
            .into_inner()
            .expect("file pointers mutex poisoned");
        let symbol_records = Arc::try_unwrap(symbol_records)
            .expect("symbol records still have outstanding references")
            .into_inner()
            .expect("symbol records mutex poisoned");
        let reference_records = Arc::try_unwrap(reference_records)
            .expect("reference records still have outstanding references")
            .into_inner()
            .expect("reference records mutex poisoned");
        let chunk_mappings = Arc::try_unwrap(chunk_mappings)
            .expect("chunk mappings still have outstanding references")
            .into_inner()
            .expect("chunk mappings mutex poisoned");

        let mut report = IndexReport::default();
        report.content_blobs = content_blobs;
        report.file_pointers = file_pointers;
        report.symbol_records = symbol_records;
        report.reference_records = reference_records;

        if let Some(branch) = &self.config.branch {
            report.branches.push(BranchHead {
                repository: self.config.repository.clone(),
                branch: branch.clone(),
                commit_sha: self.config.commit.clone(),
            });
        }

        Ok(IndexArtifacts::new(report, chunk_store, chunk_mappings))
    }

    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }
}

struct FileEntry {
    absolute: PathBuf,
    relative: PathBuf,
}

struct ChunkWrite {
    hash: String,
    text_content: String,
}

struct FileArtifacts {
    content_blob: ContentBlob,
    file_pointer: FilePointer,
    symbol_records: Vec<SymbolRecord>,
    reference_records: Vec<ReferenceRecord>,
    chunk_mappings: Vec<ChunkMapping>,
    chunk_writes: Vec<ChunkWrite>,
}

fn process_file(config: &IndexerConfig, entry: &FileEntry) -> Result<FileArtifacts> {
    let bytes = fs::read(&entry.absolute)
        .with_context(|| format!("failed to read {}", entry.absolute.display()))?;

    let content_hash = utils::compute_content_hash(&bytes);
    let language = utils::infer_language(&entry.relative).map(|s| s.to_string());
    let normalized_path = utils::normalize_relative_path(&entry.relative);
    let byte_len = bytes.len() as i64;
    let line_count = utils::line_count(&bytes);

    let mut chunk_mappings = Vec::new();
    let mut chunk_writes = Vec::new();

    let is_binary = bytes.iter().any(|&b| b == 0);
    if !is_binary {
        match std::str::from_utf8(&bytes) {
            Ok(full_text) => {
                if bytes.len() < MIN_CHUNK_SIZE as usize {
                    let chunk_hash = utils::compute_content_hash(&bytes);
                    chunk_mappings.push(ChunkMapping {
                        content_hash: content_hash.clone(),
                        chunk_hash: chunk_hash.clone(),
                        chunk_index: 0,
                        chunk_line_count: utils::line_count(&bytes),
                    });
                    chunk_writes.push(ChunkWrite {
                        hash: chunk_hash,
                        text_content: full_text.to_string(),
                    });
                } else {
                    let (chunk_ranges, used_fallback) = compute_chunk_ranges(&bytes, full_text);

                    if used_fallback {
                        debug!(
                            file = %normalized_path,
                            "fallback chunking used due to invalid UTF-8 slice"
                        );
                    }

                    let mut chunk_index = 0;
                    for (start, end) in chunk_ranges {
                        if start >= end || end > bytes.len() {
                            continue;
                        }

                        let chunk_content_bytes = &bytes[start..end];
                        let chunk_hash = utils::compute_content_hash(chunk_content_bytes);

                        if let Ok(text_content) = std::str::from_utf8(chunk_content_bytes) {
                            let line_count = utils::line_count(chunk_content_bytes);
                            chunk_mappings.push(ChunkMapping {
                                content_hash: content_hash.clone(),
                                chunk_hash: chunk_hash.clone(),
                                chunk_index,
                                chunk_line_count: line_count,
                            });
                            chunk_writes.push(ChunkWrite {
                                hash: chunk_hash,
                                text_content: text_content.to_string(),
                            });
                            chunk_index += 1;
                        } else {
                            warn!(
                                file = %normalized_path,
                                start,
                                end,
                                "skipping chunk that remained invalid UTF-8 after fallback"
                            );
                        }
                    }
                }
            }
            Err(err) => {
                debug!(
                    error = %err,
                    file = %normalized_path,
                    "skipping chunking for file with invalid UTF-8 content"
                );
            }
        }
    }

    let content_blob = ContentBlob {
        hash: content_hash.clone(),
        language: language.clone(),
        byte_len,
        line_count,
    };

    let file_pointer = FilePointer {
        repository: config.repository.clone(),
        commit_sha: config.commit.clone(),
        file_path: normalized_path.clone(),
        content_hash: content_hash.clone(),
    };

    let (symbol_records, reference_records) = match language {
        Some(ref lang) => {
            let source = String::from_utf8_lossy(&bytes);
            let namespace_hint = utils::namespace_from_path(Some(lang), &entry.relative);
            let extraction = extractors::extract(lang, &source, namespace_hint.as_deref());

            let symbols = derive_symbols(&extraction.references)
                .into_iter()
                .map(|ExtractedSymbol { name }| SymbolRecord {
                    content_hash: content_hash.clone(),
                    name,
                })
                .collect();

            let references = extraction
                .references
                .into_iter()
                .map(|reference| {
                    let namespace = reference.namespace.or_else(|| namespace_hint.clone());
                    let fully_qualified = match &namespace {
                        Some(ns) => format!("{}::{}", ns, reference.name),
                        None => reference.name.clone(),
                    };

                    ReferenceRecord {
                        content_hash: content_hash.clone(),
                        namespace,
                        name: reference.name,
                        fully_qualified,
                        kind: reference.kind,
                        line: reference.line,
                        column: reference.column,
                    }
                })
                .collect();

            (symbols, references)
        }
        None => (Vec::new(), Vec::new()),
    };

    Ok(FileArtifacts {
        content_blob,
        file_pointer,
        symbol_records,
        reference_records,
        chunk_mappings,
        chunk_writes,
    })
}

use crate::extractors::ExtractedReference;

fn derive_symbols(references: &[ExtractedReference]) -> Vec<ExtractedSymbol> {
    let mut symbols = Vec::new();
    let mut seen_symbols = HashSet::new();

    for reference in references {
        if seen_symbols.insert(&reference.name) {
            symbols.push(ExtractedSymbol {
                name: reference.name.clone(),
            });
        }
    }

    symbols
}

fn should_skip(path: &Path) -> bool {
    path.components().any(|component| {
        component
            .as_os_str()
            .to_str()
            .map(|s| matches!(s, "target" | "node_modules" | ".git"))
            .unwrap_or(false)
    })
}

fn compute_chunk_ranges(bytes: &[u8], full_text: &str) -> (Vec<(usize, usize)>, bool) {
    let fastcdc_ranges = fastcdc_chunk_ranges(bytes);
    let mut valid = true;

    for (start, end) in &fastcdc_ranges {
        if start >= end || *end > bytes.len() {
            continue;
        }

        if std::str::from_utf8(&bytes[*start..*end]).is_err() {
            valid = false;
            break;
        }
    }

    if valid {
        (fastcdc_ranges, false)
    } else {
        let fallback = fallback_chunk_ranges(full_text);
        (fallback, true)
    }
}

fn fastcdc_chunk_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
    if bytes.is_empty() {
        return Vec::new();
    }

    let mut boundaries: Vec<u64> = vec![0];
    let chunker = fastcdc::v2020::StreamCDC::new(
        Cursor::new(bytes),
        MIN_CHUNK_SIZE,
        AVG_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
    );

    for result in chunker {
        if let Ok(chunk) = result {
            boundaries.push(chunk.offset + chunk.length as u64);
        }
    }

    let total_len = bytes.len() as u64;
    if boundaries.last() != Some(&total_len) {
        boundaries.push(total_len);
    }

    let mut adjusted: Vec<u64> = vec![0];
    if boundaries.len() > 1 {
        for boundary in boundaries
            .iter()
            .skip(1)
            .take(boundaries.len().saturating_sub(2))
        {
            if *boundary >= total_len {
                continue;
            }

            if let Some(newline_pos) = bytes[*boundary as usize..].iter().position(|&b| b == b'\n')
            {
                adjusted.push(boundary + (newline_pos + 1) as u64);
            } else {
                adjusted.push(*boundary);
            }
        }
    }

    if adjusted.last() != Some(&total_len) {
        adjusted.push(total_len);
    }

    let mut ranges = Vec::new();
    for window in adjusted.windows(2) {
        let start = window[0] as usize;
        let end = window[1] as usize;
        if start < end {
            ranges.push((start, end));
        }
    }

    ranges
}

fn fallback_chunk_ranges(full_text: &str) -> Vec<(usize, usize)> {
    if full_text.is_empty() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut chunk_start = 0usize;
    let mut last_newline: Option<usize> = None;

    for (idx, ch) in full_text.char_indices() {
        let next_idx = idx + ch.len_utf8();

        if ch == '\n' {
            last_newline = Some(next_idx);
        }

        let span = next_idx - chunk_start;
        if span >= AVG_CHUNK_SIZE as usize {
            if let Some(newline_idx) = last_newline {
                ranges.push((chunk_start, newline_idx));
                chunk_start = newline_idx;
                last_newline = None;
            } else if span >= MAX_CHUNK_SIZE as usize {
                ranges.push((chunk_start, next_idx));
                chunk_start = next_idx;
                last_newline = None;
            }
        }
    }

    if chunk_start < full_text.len() {
        ranges.push((chunk_start, full_text.len()));
    }

    ranges
}
