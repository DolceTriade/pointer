use std::collections::HashSet;
use std::fs;
use std::io::Cursor;
use std::path::Path;

use anyhow::Result;
use ignore::WalkBuilder;
use tracing::{debug, trace, warn};

use crate::config::IndexerConfig;
use crate::extractors::{self, ExtractedSymbol, Extraction};
use crate::models::{
    BranchHead, ChunkMapping, ContentBlob, FilePointer, IndexArtifacts, IndexReport,
    ReferenceRecord, SymbolRecord, UniqueChunk,
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
        let mut report = IndexReport::default();
        let mut unique_chunks = HashSet::new();
        let mut chunk_mappings = Vec::new();
        let mut seen_hashes = HashSet::new();

        let walker = WalkBuilder::new(&self.config.repo_path)
            .git_ignore(true)
            .git_exclude(true)
            .hidden(false)
            .ignore(true)
            .build();

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

            let absolute_path = entry.path();
            let relative_path = match utils::ensure_relative(absolute_path, &self.config.repo_path)
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

            let bytes = match fs::read(absolute_path) {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!(error = %err, path = %absolute_path.display(), "failed to read file");
                    continue;
                }
            };

            let content_hash = utils::compute_content_hash(&bytes);
            let language = utils::infer_language(&relative_path).map(|s| s.to_string());
            let normalized_path = utils::normalize_relative_path(&relative_path);
            let byte_len = bytes.len() as i64;
            let line_count = utils::line_count(&bytes);

            if seen_hashes.insert(content_hash.clone()) {
                report.content_blobs.push(ContentBlob {
                    hash: content_hash.clone(),
                    language: language.clone(),
                    byte_len,
                    line_count,
                });

                let is_binary = bytes.iter().any(|&b| b == 0);
                if !is_binary {
                    match std::str::from_utf8(&bytes) {
                        Ok(full_text) => {
                            if bytes.len() < MIN_CHUNK_SIZE as usize {
                                // Treat small files as a single chunk
                                let chunk_hash = utils::compute_content_hash(&bytes);
                                unique_chunks.insert(UniqueChunk {
                                    chunk_hash: chunk_hash.clone(),
                                    text_content: full_text.to_string(),
                                });
                                chunk_mappings.push(ChunkMapping {
                                    content_hash: content_hash.clone(),
                                    chunk_hash,
                                    chunk_index: 0,
                                    chunk_line_count: utils::line_count(&bytes),
                                });
                            } else {
                                let (chunk_ranges, used_fallback) =
                                    compute_chunk_ranges(&bytes, full_text);

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
                                    let chunk_hash =
                                        utils::compute_content_hash(chunk_content_bytes);

                                    if let Ok(text_content) =
                                        std::str::from_utf8(chunk_content_bytes)
                                    {
                                        unique_chunks.insert(UniqueChunk {
                                            chunk_hash: chunk_hash.clone(),
                                            text_content: text_content.to_string(),
                                        });

                                        let line_count = utils::line_count(chunk_content_bytes);
                                        chunk_mappings.push(ChunkMapping {
                                            content_hash: content_hash.clone(),
                                            chunk_hash,
                                            chunk_index,
                                            chunk_line_count: line_count,
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
            }

            report.file_pointers.push(FilePointer {
                repository: self.config.repository.clone(),
                commit_sha: self.config.commit.clone(),
                file_path: normalized_path.clone(),
                content_hash: content_hash.clone(),
            });

            if let Some(ref lang) = language {
                let source = String::from_utf8_lossy(&bytes);
                let namespace_hint = utils::namespace_from_path(Some(lang), &relative_path);

                let Extraction {
                    symbols,
                    references,
                } = extractors::extract(lang, &source);
                if symbols.is_empty() {
                    debug!(file = %normalized_path, language = lang, "no symbols extracted");
                }

                for ExtractedSymbol {
                    name,
                    kind: _,
                    namespace: symbol_namespace,
                } in symbols
                {
                    let namespace = symbol_namespace.or_else(|| namespace_hint.clone());
                    let fully_qualified = match &namespace {
                        Some(ns) => format!("{}::{}", ns, name),
                        None => name.clone(),
                    };

                    report.symbol_records.push(SymbolRecord {
                        content_hash: content_hash.clone(),
                        namespace,
                        name,
                        fully_qualified,
                    });
                }

                for reference in references {
                    let namespace = reference.namespace.or_else(|| namespace_hint.clone());
                    let fully_qualified = match &namespace {
                        Some(ns) => format!("{}::{}", ns, reference.name),
                        None => reference.name.clone(),
                    };

                    report.reference_records.push(ReferenceRecord {
                        content_hash: content_hash.clone(),
                        namespace,
                        name: reference.name,
                        fully_qualified,
                        kind: reference.kind,
                        line: reference.line,
                        column: reference.column,
                    });
                }
            }
        }

        if let Some(branch) = &self.config.branch {
            report.branches.push(BranchHead {
                repository: self.config.repository.clone(),
                branch: branch.clone(),
                commit_sha: self.config.commit.clone(),
            });
        }

        Ok(IndexArtifacts {
            report,
            unique_chunks: unique_chunks.into_iter().collect(),
            chunk_mappings,
        })
    }

    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }
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
