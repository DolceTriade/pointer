use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::{Context, Result};
use crossbeam_channel::bounded;
use ignore::{WalkBuilder, WalkState};
use rayon::iter::ParallelBridge;
use rayon::prelude::*;
use tracing::{debug, trace, warn};

use crate::chunk_store::ChunkStore;
use crate::config::IndexerConfig;
use crate::extractors::{self, ExtractedSymbol};
use crate::models::{
    BranchHead, BranchPolicy, BranchSnapshotPolicy, ChunkMapping, ContentBlob, FilePointer,
    IndexArtifacts, RecordWriter, ReferenceRecord, SymbolNamespaceRecord, SymbolRecord,
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
            .build_parallel();

        let scratch_dir = self.config.output_dir.join(".pointer-scratch");
        fs::create_dir_all(&scratch_dir).with_context(|| {
            format!(
                "failed to create scratch directory {}",
                scratch_dir.display()
            )
        })?;

        let (tx, rx) = bounded::<FileEntry>(1024);
        let walker_thread = {
            let tx = tx.clone();
            let repo_root = self.config.repo_path.clone();
            thread::spawn(move || {
                walker.run(|| {
                    let tx = tx.clone();
                    let repo_root = repo_root.clone();
                    Box::new(move |entry| {
                        match entry {
                            Ok(entry) => {
                                if !entry
                                    .file_type()
                                    .map(|ft| ft.is_file())
                                    .unwrap_or(false)
                                {
                                    trace!(path = %entry.path().display(), "skipping non-file entry");
                                    return WalkState::Continue;
                                }

                                let absolute_path = entry.path().to_path_buf();
                                let relative_path =
                                    match utils::ensure_relative(&absolute_path, &repo_root) {
                                        Ok(path) => path,
                                        Err(err) => {
                                            warn!(
                                                error = %err,
                                                path = %absolute_path.display(),
                                                "skipping file outside repo root"
                                            );
                                            return WalkState::Continue;
                                        }
                                    };

                                if should_skip(&relative_path) {
                                    trace!(path = %relative_path.display(), "skipping filtered file");
                                    return WalkState::Continue;
                                }

                                if tx
                                    .send(FileEntry {
                                        absolute: absolute_path,
                                        relative: relative_path,
                                    })
                                    .is_err()
                                {
                                    return WalkState::Quit;
                                }
                            }
                            Err(err) => {
                                warn!(error = %err, "failed to read directory entry");
                            }
                        }
                        WalkState::Continue
                    })
                });
            })
        };
        drop(tx);

        let chunk_store = Arc::new(Mutex::new(ChunkStore::new_in(&scratch_dir)?));
        let seen_hashes = Arc::new(Mutex::new(HashSet::new()));
        let content_blobs_writer = RecordWriter::<ContentBlob>::new_in(&scratch_dir)?;
        let file_pointers_writer = RecordWriter::<FilePointer>::new_in(&scratch_dir)?;
        let symbol_records_writer = RecordWriter::<SymbolRecord>::new_in(&scratch_dir)?;
        let symbol_namespaces_writer = RecordWriter::<SymbolNamespaceRecord>::new_in(&scratch_dir)?;
        let reference_records_writer = RecordWriter::<ReferenceRecord>::new_in(&scratch_dir)?;
        let chunk_mappings_writer = RecordWriter::<ChunkMapping>::new_in(&scratch_dir)?;
        let seen_namespaces = Arc::new(Mutex::new(HashSet::new()));

        let config = self.config.clone();

        rx.into_iter()
            .par_bridge()
            .for_each({
                let chunk_store = chunk_store.clone();
                let seen_hashes = seen_hashes.clone();
                let content_blobs_writer = content_blobs_writer.clone();
                let file_pointers_writer = file_pointers_writer.clone();
                let symbol_records_writer = symbol_records_writer.clone();
                let symbol_namespaces_writer = symbol_namespaces_writer.clone();
                let reference_records_writer = reference_records_writer.clone();
                let chunk_mappings_writer = chunk_mappings_writer.clone();
                let seen_namespaces = seen_namespaces.clone();
                let config = config.clone();

                move |entry| match process_file(&config, &entry) {
                    Ok(file_artifacts) => {
                        let FileArtifacts {
                            content_blob,
                            file_pointer,
                            symbol_records: file_symbols,
                            symbol_namespaces: file_namespaces,
                            reference_records: file_references,
                            chunk_mappings: file_chunk_mappings,
                            chunk_writes,
                        } = file_artifacts;

                        let content_hash = file_pointer.content_hash.clone();

                        if let Err(err) = file_pointers_writer.append(&file_pointer) {
                            warn!(error = %err, "failed to record file pointer");
                        }

                        let is_new_content = {
                            let mut seen =
                                seen_hashes.lock().expect("seen hashes mutex poisoned");
                            seen.insert(content_hash.clone())
                        };

                        if is_new_content {
                            if let Err(err) = content_blobs_writer.append(&content_blob) {
                                warn!(error = %err, %content_hash, "failed to record content blob");
                            }

                            for mapping in &file_chunk_mappings {
                                if let Err(err) = chunk_mappings_writer.append(mapping) {
                                    warn!(
                                        error = %err,
                                        %content_hash,
                                        "failed to record chunk mapping"
                                    );
                                }
                            }

                            for symbol in &file_symbols {
                                if let Err(err) = symbol_records_writer.append(symbol) {
                                    warn!(
                                        error = %err,
                                        %content_hash,
                                        "failed to record symbol"
                                    );
                                }
                            }

                            for namespace in &file_namespaces {
                                let ns = namespace.namespace.clone();
                                let should_write = {
                                    let mut guard =
                                        seen_namespaces.lock().expect("namespace set mutex poisoned");
                                    guard.insert(ns.clone())
                                };
                                if should_write {
                                    if let Err(err) = symbol_namespaces_writer.append(namespace) {
                                        warn!(error = %err, namespace = %ns, "failed to record namespace");
                                    }
                                }
                            }

                            for reference in &file_references {
                                if let Err(err) = reference_records_writer.append(reference) {
                                    warn!(
                                        error = %err,
                                        %content_hash,
                                        "failed to record reference"
                                    );
                                }
                            }

                            let mut store =
                                chunk_store.lock().expect("chunk store mutex poisoned");
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
                }
            });

        walker_thread.join().expect("file walker thread panicked");

        let chunk_store = Arc::try_unwrap(chunk_store)
            .expect("chunk store still has outstanding references")
            .into_inner()
            .expect("chunk store mutex poisoned");
        let content_blobs = content_blobs_writer.into_store()?;
        let file_pointers = file_pointers_writer.into_store()?;
        let symbol_records = symbol_records_writer.into_store()?;
        let symbol_namespaces = symbol_namespaces_writer.into_store()?;
        let reference_records = reference_records_writer.into_store()?;
        let chunk_mappings = chunk_mappings_writer.into_store()?;

        let mut branches = Vec::new();
        if let Some(branch) = &self.config.branch {
            let policy = self
                .config
                .branch_policy
                .clone()
                .map(|policy| BranchPolicy {
                    latest_keep_count: policy.latest_keep_count,
                    is_live: policy.live,
                    snapshot_policies: policy
                        .snapshot_policies
                        .into_iter()
                        .map(|snapshot| BranchSnapshotPolicy {
                            interval_seconds: snapshot.interval_seconds,
                            keep_count: snapshot.keep_count,
                        })
                        .collect(),
                });
            branches.push(BranchHead {
                repository: self.config.repository.clone(),
                branch: branch.clone(),
                commit_sha: self.config.commit.clone(),
                policy,
            });
        }

        Ok(IndexArtifacts::new(
            content_blobs,
            symbol_records,
            symbol_namespaces,
            file_pointers,
            reference_records,
            chunk_mappings,
            chunk_store,
            branches,
            scratch_dir,
        ))
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
    symbol_namespaces: Vec<SymbolNamespaceRecord>,
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

    let (symbol_records, reference_records, symbol_namespaces) = match language {
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

            let references: Vec<ReferenceRecord> = extraction
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

            let mut namespace_set = HashSet::new();
            let mut namespaces = Vec::new();
            for reference in &references {
                let ns = reference.namespace.clone().unwrap_or_default();
                if namespace_set.insert(ns.clone()) {
                    namespaces.push(SymbolNamespaceRecord { namespace: ns });
                }
            }

            (symbols, references, namespaces)
        }
        None => (Vec::new(), Vec::new(), Vec::new()),
    };

    Ok(FileArtifacts {
        content_blob,
        file_pointer,
        symbol_records,
        symbol_namespaces,
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
