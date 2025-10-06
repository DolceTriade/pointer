use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::Result;
use blake3::Hasher;
use fastcdc::FastCDC;
use ignore::WalkBuilder;
use tracing::{debug, trace, warn};

use crate::config::IndexerConfig;
use crate::extractors::{self, ExtractedSymbol, Extraction};
use crate::models::{
    ChunkDescriptor, ChunkPayload, ContentBlob, FileChunkRecord, FilePointer, IndexArtifacts,
    IndexReport, ReferenceRecord, SymbolRecord,
};
use crate::utils;

const SMALL_FILE_THRESHOLD: usize = 8 * 1024; // 8 KiB
const CDC_MIN: usize = 2 * 1024; // 2 KiB
const CDC_AVG: usize = 16 * 1024; // 16 KiB
const CDC_MAX: usize = 32 * 1024; // 32 KiB
const CHUNK_HASH_ALGORITHM: &str = "blake3";

pub struct Indexer {
    config: IndexerConfig,
}

impl Indexer {
    pub fn new(config: IndexerConfig) -> Self {
        Self { config }
    }

    pub fn run(&self) -> Result<IndexArtifacts> {
        let mut report = IndexReport::default();
        let mut seen_hashes = HashSet::new();
        let mut chunk_payloads: HashMap<String, ChunkPayload> = HashMap::new();
        let mut chunk_descriptors: HashMap<String, ChunkDescriptor> = HashMap::new();

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
            }

            report.file_pointers.push(FilePointer {
                repository: self.config.repository.clone(),
                commit_sha: self.config.commit.clone(),
                file_path: normalized_path.clone(),
                content_hash: content_hash.clone(),
            });

            if !bytes.is_empty() {
                let newline_offsets: Vec<usize> = bytes
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, b)| if *b == b'\n' { Some(idx) } else { None })
                    .collect();

                let chunk_specs: Vec<(usize, usize)> = if bytes.len() <= SMALL_FILE_THRESHOLD {
                    vec![(0, bytes.len())]
                } else {
                    FastCDC::new(&bytes, CDC_MIN, CDC_AVG, CDC_MAX)
                        .map(|chunk| (chunk.offset as usize, chunk.length as usize))
                        .collect()
                };

                for (sequence, (offset, length)) in chunk_specs.into_iter().enumerate() {
                    if length == 0 {
                        continue;
                    }

                    let end = offset + length;
                    let slice = &bytes[offset..end];
                    let mut hasher = Hasher::new();
                    hasher.update(slice);
                    let hash = hasher.finalize().to_hex().to_string();

                    chunk_payloads
                        .entry(hash.clone())
                        .or_insert_with(|| ChunkPayload {
                            hash: hash.clone(),
                            algorithm: CHUNK_HASH_ALGORITHM.to_string(),
                            data: slice.to_vec(),
                        });

                    chunk_descriptors
                        .entry(hash.clone())
                        .or_insert_with(|| ChunkDescriptor {
                            hash: hash.clone(),
                            algorithm: CHUNK_HASH_ALGORITHM.to_string(),
                            byte_len: slice.len() as u32,
                        });

                    let start_line = line_number_at_offset(&newline_offsets, offset);
                    let end_line = line_number_at_offset(&newline_offsets, end.saturating_sub(1));
                    let line_count = end_line.saturating_sub(start_line) + 1;

                    report.file_chunks.push(FileChunkRecord {
                        repository: self.config.repository.clone(),
                        commit_sha: self.config.commit.clone(),
                        file_path: normalized_path.clone(),
                        sequence: sequence as u32,
                        chunk_hash: hash,
                        byte_offset: offset as u64,
                        byte_len: slice.len() as u32,
                        start_line,
                        line_count,
                    });
                }
            }

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

        let mut descriptors: Vec<ChunkDescriptor> = chunk_descriptors.into_values().collect();
        descriptors.sort_by(|a, b| a.hash.cmp(&b.hash));
        report.chunk_descriptors = descriptors;

        let mut chunks: Vec<ChunkPayload> = chunk_payloads.into_values().collect();
        chunks.sort_by(|a, b| a.hash.cmp(&b.hash));

        Ok(IndexArtifacts { report, chunks })
    }

    pub fn config(&self) -> &IndexerConfig {
        &self.config
    }
}

fn line_number_at_offset(newlines: &[usize], offset: usize) -> u32 {
    if newlines.is_empty() {
        return 1;
    }

    let mut lo = 0usize;
    let mut hi = newlines.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if newlines[mid] < offset {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    (lo as u32) + 1
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
