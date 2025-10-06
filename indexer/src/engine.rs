use std::collections::HashSet;
use std::fs;
use std::path::Path;

use anyhow::Result;
use ignore::WalkBuilder;
use tracing::{debug, trace, warn};

use crate::config::IndexerConfig;
use crate::extractors::{self, ExtractedSymbol, Extraction};
use crate::models::{ContentBlob, FilePointer, IndexReport, ReferenceRecord, SymbolRecord};
use crate::utils;

pub struct Indexer {
    config: IndexerConfig,
}

impl Indexer {
    pub fn new(config: IndexerConfig) -> Self {
        Self { config }
    }

    pub fn run(&self) -> Result<IndexReport> {
        let mut report = IndexReport::default();
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
                    warn!(error = %err, path = %absolute_path.display(), "skipping file outside repo root");
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

        Ok(report)
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
