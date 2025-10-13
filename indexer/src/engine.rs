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
    ContentBlob, ChunkMapping, FilePointer, IndexArtifacts, IndexReport, ReferenceRecord,
    SymbolRecord, UniqueChunk,
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
            let relative_path =
                match utils::ensure_relative(absolute_path, &self.config.repo_path) {
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
                    if bytes.len() < MIN_CHUNK_SIZE as usize {
                        // Treat small files as a single chunk
                        let chunk_hash = utils::compute_content_hash(&bytes);
                        unique_chunks.insert(UniqueChunk {
                            chunk_hash: chunk_hash.clone(),
                            text_content: String::from_utf8_lossy(&bytes).to_string(),
                        });
                        chunk_mappings.push(ChunkMapping {
                            content_hash: content_hash.clone(),
                            chunk_hash,
                            chunk_index: 0,
                            chunk_line_count: utils::line_count(&bytes),
                        });
                    } else {
                        // Use fastcdc for large files, aligning to newlines
                        let mut boundaries: Vec<u64> = vec![0];
                        let chunker = fastcdc::v2020::StreamCDC::new(
                            Cursor::new(&bytes),
                            MIN_CHUNK_SIZE,
                            AVG_CHUNK_SIZE,
                            MAX_CHUNK_SIZE,
                        );

                        for result in chunker {
                            if let Ok(chunk) = result {
                                boundaries.push(chunk.offset + chunk.length as u64);
                            }
                        }

                        if boundaries.last() != Some(&(bytes.len() as u64)) {
                            boundaries.push(bytes.len() as u64);
                        }

                        let mut adjusted_boundaries: Vec<u64> = vec![0];
                        for &boundary in &boundaries[1..boundaries.len() - 1] {
                            if boundary >= bytes.len() as u64 {
                                continue;
                            }
                            if let Some(newline_pos) =
                                bytes[boundary as usize..].iter().position(|&b| b == b'\n')
                            {
                                adjusted_boundaries.push(boundary + (newline_pos + 1) as u64);
                            } else {
                                adjusted_boundaries.push(boundary);
                            }
                        }
                        if adjusted_boundaries.last() != Some(&(bytes.len() as u64)) {
                            adjusted_boundaries.push(bytes.len() as u64);
                        }

                        let mut chunk_index = 0;
                        for i in 0..adjusted_boundaries.len() - 1 {
                            let start = adjusted_boundaries[i];
                            let end = adjusted_boundaries[i + 1];

                            if start >= end {
                                continue;
                            }

                            let chunk_content_bytes = &bytes[start as usize..end as usize];
                            let chunk_hash = utils::compute_content_hash(chunk_content_bytes);

                            unique_chunks.insert(UniqueChunk {
                                chunk_hash: chunk_hash.clone(),
                                text_content: String::from_utf8_lossy(chunk_content_bytes)
                                    .to_string(),
                            });

                            let line_count = utils::line_count(chunk_content_bytes);
                            chunk_mappings.push(ChunkMapping {
                                content_hash: content_hash.clone(),
                                chunk_hash,
                                chunk_index,
                                chunk_line_count: line_count,
                            });

                            chunk_index += 1;
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
                    kind,
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
                        symbol: name,
                        fully_qualified,
                        kind: Some(kind),
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
