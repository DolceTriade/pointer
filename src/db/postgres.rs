use crate::db::models::FileReference as DbFileReference;
use crate::db::{
    ChunkUploadItem, Database, DbError, FileContentResponse, FileReference, HighlightedLine,
    HighlightedSegment, ReferenceResult, RepoSummary, RepoTreeQuery, SearchRequest, SearchResponse,
    SnippetRequest, SnippetResponse, SymbolReferenceRequest, SymbolReferenceResponse, SymbolResult,
    TokenOccurrence, TreeEntry, TreeResponse,
};
use async_trait::async_trait;
use base64::Engine;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::io::Read;

#[derive(Clone)]
pub struct PostgresDb {
    pool: PgPool,
}

impl PostgresDb {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Database for PostgresDb {
    async fn get_all_repositories(&self) -> Result<Vec<RepoSummary>, DbError> {
        let rows: Vec<(String, i64)> = sqlx::query_as(
            "SELECT repository, COUNT(*) as file_count FROM files GROUP BY repository ORDER BY repository",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        let repos = rows
            .into_iter()
            .map(|(repository, file_count)| RepoSummary {
                repository,
                file_count,
            })
            .collect();

        Ok(repos)
    }

    async fn get_branches_for_repository(&self, repository: &str) -> Result<Vec<String>, DbError> {
        // Note: The backend binary stores commits, not branches
        // In Git, branches are references to commits
        let commits: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 ORDER BY commit_sha DESC",
        )
        .bind(repository)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(commits)
    }

    async fn chunk_need(&self, hashes: Vec<String>) -> Result<Vec<String>, DbError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let existing: Vec<(String,)> =
            sqlx::query_as("SELECT hash FROM chunks WHERE hash = ANY($1)")
                .bind(&hashes)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

        let present: std::collections::HashSet<String> =
            existing.into_iter().map(|row| row.0).collect();
        let requested: std::collections::HashSet<String> = hashes.into_iter().collect();
        let missing: Vec<String> = requested.difference(&present).cloned().collect();

        Ok(missing)
    }

    async fn chunk_upload(&self, chunks: Vec<ChunkUploadItem>) -> Result<(), DbError> {
        if chunks.is_empty() {
            return Ok(());
        }

        let mut decoded = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let data = base64::engine::general_purpose::STANDARD
                .decode(&chunk.data)
                .map_err(|e| DbError::Internal(format!("invalid base64 data: {}", e)))?;

            if chunk.byte_len != data.len() as u32 {
                log::warn!(
                    "hash={}, expected={}, actual={}, chunk length mismatch; using decoded length",
                    chunk.hash,
                    chunk.byte_len,
                    data.len()
                );
            }

            decoded.push(DecodedChunk {
                hash: chunk.hash,
                algorithm: chunk.algorithm,
                byte_len: data.len() as u32,
                data,
            });
        }

        let deduped = dedup_by_key(&decoded, |chunk| chunk.hash.clone());

        for batch in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new("INSERT INTO chunks (hash, algorithm, byte_len, data) ");
            qb.push_values(batch.iter().copied(), |mut b, chunk| {
                let byte_len: i32 = chunk.byte_len.try_into().unwrap_or(i32::MAX);
                b.push_bind(&chunk.hash)
                    .push_bind(&chunk.algorithm)
                    .push_bind(byte_len)
                    .push_bind(&chunk.data);
            });
            qb.push(" ON CONFLICT (hash) DO NOTHING");

            qb.build()
                .execute(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }

    async fn store_manifest_chunk(
        &self,
        upload_id: String,
        chunk_index: i32,
        total_chunks: i32,
        data: Vec<u8>,
    ) -> Result<(), DbError> {
        if chunk_index < 0 || total_chunks <= 0 || chunk_index >= total_chunks {
            return Err(DbError::Internal(
                "invalid manifest chunk metadata".to_string(),
            ));
        }

        sqlx::query(
            "INSERT INTO upload_chunks (upload_id, chunk_index, total_chunks, data)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (upload_id, chunk_index) DO UPDATE
             SET total_chunks = EXCLUDED.total_chunks, data = EXCLUDED.data",
        )
        .bind(&upload_id)
        .bind(chunk_index)
        .bind(total_chunks)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(())
    }

    async fn finalize_manifest(
        &self,
        upload_id: String,
        compressed: Option<bool>,
    ) -> Result<(), DbError> {
        use zstd::stream::read::Decoder;

        let rows: Vec<UploadChunkRow> = sqlx::query_as(
            "SELECT chunk_index, total_chunks, data FROM upload_chunks WHERE upload_id = $1 ORDER BY chunk_index",
        )
        .bind(&upload_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if rows.is_empty() {
            return Err(DbError::Internal(
                "no chunks uploaded for manifest".to_string(),
            ));
        }

        let expected_total = rows[0].total_chunks;
        if expected_total <= 0 {
            return Err(DbError::Internal("invalid total chunk count".to_string()));
        }

        if rows.len() != expected_total as usize {
            return Err(DbError::Internal("missing manifest chunks".to_string()));
        }

        for (index, row) in rows.iter().enumerate() {
            if row.chunk_index != index as i32 || row.total_chunks != expected_total {
                return Err(DbError::Internal(
                    "inconsistent manifest chunk metadata".to_string(),
                ));
            }
        }

        let mut combined = Vec::with_capacity(rows.iter().map(|row| row.data.len()).sum());
        for row in rows {
            combined.extend_from_slice(&row.data);
        }

        let compressed = compressed.unwrap_or(false);
        let report_bytes = if compressed {
            let cursor = std::io::Cursor::new(combined);
            let mut decoder =
                Decoder::new(cursor).map_err(|e| DbError::Compression(e.to_string()))?;
            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e: std::io::Error| DbError::Compression(e.to_string()))?;
            buf
        } else {
            combined
        };

        let report: pointer_indexer::models::IndexReport = serde_json::from_slice(&report_bytes)
            .map_err(|e| DbError::Serialization(e.to_string()))?;

        self.ingest_report(report).await?;

        sqlx::query("DELETE FROM upload_chunks WHERE upload_id = $1")
            .bind(&upload_id)
            .execute(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(())
    }

    async fn list_commits(&self, repository: &str) -> Result<Vec<String>, DbError> {
        let commits: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 ORDER BY commit_sha DESC",
        )
        .bind(repository)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(commits)
    }

    async fn get_repo_tree(
        &self,
        repository: &str,
        query: RepoTreeQuery,
    ) -> Result<TreeResponse, DbError> {
        if query.commit.is_empty() {
            return Err(DbError::Internal("missing commit parameter".to_string()));
        }

        let prefix = query.path.unwrap_or_default();
        let normalized_prefix = prefix.trim_matches('/');

        let like_pattern = if normalized_prefix.is_empty() {
            "%".to_string()
        } else {
            format!(
                "{}%",
                normalized_prefix.trim_start_matches('/').to_string() + "/"
            )
        };

        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT file_path FROM files WHERE repository = $1 AND commit_sha = $2 AND (file_path = $3 OR file_path LIKE $4)",
        )
        .bind(repository)
        .bind(&query.commit)
        .bind(normalized_prefix)
        .bind(like_pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if rows.is_empty() && !normalized_prefix.is_empty() {
            return Err(DbError::Internal("path not found".to_string()));
        }

        let mut directories: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut files: std::collections::HashSet<String> = std::collections::HashSet::new();

        for path in rows {
            let relative = if normalized_prefix.is_empty() {
                path.clone()
            } else if path == normalized_prefix {
                continue;
            } else {
                path.trim_start_matches(normalized_prefix)
                    .trim_start_matches('/')
                    .to_string()
            };

            if relative.is_empty() {
                continue;
            }

            if let Some((head, _)) = relative.split_once('/') {
                if !head.is_empty() {
                    let dir_path = if normalized_prefix.is_empty() {
                        head.to_string()
                    } else {
                        format!("{}/{}", normalized_prefix, head)
                    };
                    directories.insert(dir_path);
                }
            } else {
                let file_path = if normalized_prefix.is_empty() {
                    relative
                } else {
                    format!("{}/{}", normalized_prefix, relative)
                };
                files.insert(file_path);
            }
        }

        let mut entries: Vec<TreeEntry> = directories
            .into_iter()
            .map(|dir| TreeEntry {
                name: dir.rsplit('/').next().unwrap_or(&dir).to_string(),
                path: dir,
                kind: "dir".to_string(),
            })
            .collect();

        entries.extend(files.into_iter().map(|file_path| {
            TreeEntry {
                name: file_path
                    .rsplit('/')
                    .next()
                    .unwrap_or(&file_path)
                    .to_string(),
                path: file_path,
                kind: "file".to_string(),
            }
        }));

        entries.sort_by(|a, b| match (a.kind.as_str(), b.kind.as_str()) {
            ("dir", "file") => std::cmp::Ordering::Less,
            ("file", "dir") => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        });

        Ok(TreeResponse {
            repository: repository.to_string(),
            commit_sha: query.commit,
            path: normalized_prefix.to_string(),
            entries,
        })
    }

    async fn get_file_content(
        &self,
        repository: &str,
        commit_sha: &str,
        file_path: &str,
    ) -> Result<FileContentResponse, DbError> {
        if commit_sha.is_empty() {
            return Err(DbError::Internal("missing commit parameter".to_string()));
        }

        if file_path.is_empty() {
            return Err(DbError::Internal("missing file path".to_string()));
        }

        let data = self
            .load_file_data(repository, commit_sha, file_path)
            .await?;

        let text = String::from_utf8_lossy(&data.bytes).to_string();
        let highlight = self.highlight_text(&text, data.language.as_deref());
        let tokens = self.compute_tokens(&text);

        Ok(FileContentResponse {
            repository: repository.to_string(),
            commit_sha: commit_sha.to_string(),
            file_path: file_path.to_string(),
            language: data.language,
            lines: highlight,
            tokens,
        })
    }

    async fn get_file_snippet(&self, request: SnippetRequest) -> Result<SnippetResponse, DbError> {
        if request.line == 0 {
            return Err(DbError::Internal("line numbers are 1-based".to_string()));
        }

        let data = self
            .load_file_data(&request.repository, &request.commit_sha, &request.file_path)
            .await?;

        let file_text = String::from_utf8_lossy(&data.bytes);
        let lines: Vec<String> = file_text.lines().map(|line| line.to_string()).collect();

        if lines.is_empty() {
            return Err(DbError::Internal("file is empty".to_string()));
        }

        let total_lines = lines.len() as u32;
        if request.line > total_lines {
            return Err(DbError::Internal(
                "line number exceeds file length".to_string(),
            ));
        }

        let context = request.context.unwrap_or(3).min(1000);
        let start_line = if request.line <= context {
            1
        } else {
            request.line - context
        };
        let end_line = (request.line + context).min(total_lines);

        let start_index = (start_line - 1) as usize;
        let end_index = end_line as usize;
        let snippet_lines = lines[start_index..end_index]
            .iter()
            .map(|line| line.to_string())
            .collect();

        let truncated = start_line > 1 || end_line < total_lines;

        Ok(SnippetResponse {
            start_line,
            highlight_line: request.line,
            total_lines,
            lines: snippet_lines,
            truncated,
        })
    }

    async fn get_symbol_references(
        &self,
        request: SymbolReferenceRequest,
    ) -> Result<SymbolReferenceResponse, DbError> {
        let rows: Vec<DbFileReference> = sqlx::query_as(
            "SELECT f.repository, f.commit_sha, f.file_path, r.namespace, r.name, r.kind,
                    r.line_number AS line, r.column_number AS column
             FROM symbol_references r
             JOIN files f ON f.content_hash = r.content_hash
             WHERE f.repository = $1 AND f.commit_sha = $2 AND r.fully_qualified = $3
             ORDER BY f.file_path, r.line_number, r.column_number",
        )
        .bind(&request.repository)
        .bind(&request.commit_sha)
        .bind(&request.fully_qualified)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(SymbolReferenceResponse {
            references: rows
                .into_iter()
                .map(|r| FileReference {
                    repository: r.repository,
                    commit_sha: r.commit_sha,
                    file_path: r.file_path,
                    namespace: r.namespace,
                    name: r.name,
                    kind: r.kind,
                    line: r.line,
                    column: r.column,
                })
                .collect(),
        })
    }

    async fn search_symbols(&self, request: SearchRequest) -> Result<SearchResponse, DbError> {
        let mut qb = QueryBuilder::new(
            "SELECT s.symbol, s.namespace, s.kind, s.fully_qualified, cb.language,
             f.repository, f.commit_sha, f.file_path
             FROM symbols s
             JOIN content_blobs cb ON cb.hash = s.content_hash
             JOIN files f ON f.content_hash = s.content_hash
             WHERE 1 = 1",
        );

        if let Some(name) = &request.name {
            qb.push(" AND s.symbol ILIKE ")
                .push_bind(format!("%{}%", name));
        }

        if let Some(regex) = &request.name_regex {
            qb.push(" AND s.symbol ~* ").push_bind(regex);
        }

        if let Some(namespace) = &request.namespace {
            qb.push(" AND s.namespace = ").push_bind(namespace);
        }

        if let Some(prefix) = &request.namespace_prefix {
            qb.push(" AND s.namespace LIKE ")
                .push_bind(format!("{}%", prefix));
        }

        if let Some(kinds) = &request.kind {
            qb.push(" AND s.kind = ANY(").push_bind(kinds).push(")");
        }

        if let Some(languages) = &request.language {
            qb.push(" AND cb.language = ANY(")
                .push_bind(languages)
                .push(")");
        }

        if let Some(repo) = &request.repository {
            qb.push(" AND f.repository = ").push_bind(repo);
        }

        if let Some(commit) = &request.commit_sha {
            qb.push(" AND f.commit_sha = ").push_bind(commit);
        }

        if let Some(path) = &request.path {
            qb.push(" AND f.file_path ILIKE ")
                .push_bind(format!("%{}%", path));
        }

        if let Some(regex) = &request.path_regex {
            qb.push(" AND f.file_path ~* ").push_bind(regex);
        }

        let limit = request.limit.unwrap_or(100).clamp(1, 1000);
        qb.push(" ORDER BY s.symbol ASC LIMIT ").push_bind(limit);

        let rows: Vec<SymbolRow> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        let include_refs = request.include_references.unwrap_or(false);
        let mut reference_map: std::collections::HashMap<String, Vec<ReferenceRow>> =
            std::collections::HashMap::new();

        if include_refs {
            let fully_qualified: std::collections::HashSet<String> =
                rows.iter().map(|row| row.fully_qualified.clone()).collect();
            if !fully_qualified.is_empty() {
                let lookup: Vec<String> = fully_qualified.into_iter().collect();
                let ref_rows: Vec<ReferenceRow> = sqlx::query_as(
                    "SELECT fully_qualified, name, namespace, kind, line_number AS line, column_number AS column
                     FROM symbol_references WHERE fully_qualified = ANY($1)",
                )
                .bind(&lookup)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

                for reference in ref_rows {
                    reference_map
                        .entry(reference.fully_qualified.clone())
                        .or_insert_with(Vec::new)
                        .push(reference);
                }
            }
        }

        let mut results = Vec::new();

        for row in rows {
            let references = if include_refs {
                reference_map.get(row.fully_qualified.as_str()).map(|refs| {
                    refs.iter()
                        .map(|r| ReferenceResult {
                            name: r.name.clone(),
                            namespace: r.namespace.clone(),
                            kind: r.kind.clone(),
                            fully_qualified: r.fully_qualified.clone(),
                            line: r.line.max(0) as usize,
                            column: r.column.max(0) as usize,
                        })
                        .collect()
                })
            } else {
                None
            };

            results.push(SymbolResult {
                symbol: row.symbol,
                namespace: row.namespace,
                kind: row.kind,
                fully_qualified: row.fully_qualified,
                repository: row.repository,
                commit_sha: row.commit_sha,
                file_path: row.file_path,
                language: row.language,
                references,
            });
        }

        Ok(SearchResponse { symbols: results })
    }

    async fn health_check(&self) -> Result<String, DbError> {
        sqlx::query_scalar::<_, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok("ok".to_string())
    }
}

impl PostgresDb {
    async fn load_file_data(
        &self,
        repository: &str,
        commit_sha: &str,
        file_path: &str,
    ) -> Result<FileData, DbError> {
        let language_row = sqlx::query_scalar::<_, Option<String>>(
            "SELECT cb.language
             FROM files f
             LEFT JOIN content_blobs cb ON cb.hash = f.content_hash
             WHERE f.repository = $1 AND f.commit_sha = $2 AND f.file_path = $3",
        )
        .bind(repository)
        .bind(commit_sha)
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        let language = match language_row {
            Some(lang) => lang,
            None => return Err(DbError::Internal("file not found".to_string())),
        };

        let chunk_rows: Vec<FileChunkDataRow> = sqlx::query_as(
            "SELECT chunk_order, chunk_hash, byte_len
             FROM file_chunks
             WHERE repository = $1 AND commit_sha = $2 AND file_path = $3
             ORDER BY chunk_order",
        )
        .bind(repository)
        .bind(commit_sha)
        .bind(file_path)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if chunk_rows.is_empty() {
            return Err(DbError::Internal(
                "file does not have chunk data".to_string(),
            ));
        }

        let hashes: Vec<String> = chunk_rows
            .iter()
            .map(|row| row.chunk_hash.clone())
            .collect();
        let chunk_data: Vec<(String, Vec<u8>)> =
            sqlx::query_as("SELECT hash, data FROM chunks WHERE hash = ANY($1)")
                .bind(&hashes)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

        let map: std::collections::HashMap<String, Vec<u8>> = chunk_data.into_iter().collect();

        let capacity: usize = chunk_rows
            .iter()
            .map(|row| row.byte_len.max(0) as usize)
            .sum();
        let mut bytes = Vec::with_capacity(capacity);

        for row in &chunk_rows {
            let data = map.get(&row.chunk_hash).ok_or_else(|| {
                DbError::Internal(format!("missing chunk data for {}", row.chunk_hash))
            })?;
            bytes.extend_from_slice(data);
        }

        Ok(FileData { bytes, language })
    }

    fn highlight_text(&self, text: &str, _language: Option<&str>) -> Vec<HighlightedLine> {
        // For now, we'll return a simple implementation
        // In a real implementation, you'd use the same highlighting logic as in the backend
        text.lines()
            .enumerate()
            .map(|(idx, line)| {
                let segments = vec![HighlightedSegment {
                    text: line.to_string(),
                    foreground: Some("#000000".to_string()),
                    background: None,
                    bold: false,
                    italic: false,
                }];

                HighlightedLine {
                    line_number: idx as u32 + 1,
                    segments,
                }
            })
            .collect()
    }

    fn compute_tokens(&self, text: &str) -> Vec<TokenOccurrence> {
        let mut result = Vec::new();

        for (line_idx, line) in text.lines().enumerate() {
            let line_number = line_idx as u32 + 1;
            let mut column: u32 = 1;
            let mut current = String::new();
            let mut start_column: u32 = 1;

            for ch in line.chars() {
                let is_token_char = ch.is_alphanumeric() || ch == '_';
                if is_token_char {
                    if current.is_empty() {
                        start_column = column;
                    }
                    current.push(ch);
                } else if !current.is_empty() {
                    let length = current.chars().count() as u32;
                    result.push(TokenOccurrence {
                        token: current.clone(),
                        line: line_number,
                        column: start_column,
                        length,
                    });
                    current.clear();
                }
                column += 1;
            }

            if !current.is_empty() {
                let length = current.chars().count() as u32;
                result.push(TokenOccurrence {
                    token: current.clone(),
                    line: line_number,
                    column: start_column,
                    length,
                });
            }
        }

        result
    }

    async fn ingest_report(
        &self,
        report: pointer_indexer::models::IndexReport,
    ) -> Result<(), DbError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        self.insert_content_blobs(&mut tx, &report.content_blobs)
            .await?;
        self.insert_file_pointers(&mut tx, &report.file_pointers)
            .await?;
        self.insert_symbol_records(&mut tx, &report.symbol_records)
            .await?;
        self.insert_reference_records(&mut tx, &report.reference_records)
            .await?;
        self.insert_file_chunk_records(&mut tx, &report.file_chunks)
            .await?;

        tx.commit()
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(())
    }

    async fn insert_content_blobs(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        blobs: &[pointer_indexer::models::ContentBlob],
    ) -> Result<(), DbError> {
        if blobs.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(blobs, |blob| blob.hash.clone());

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new(
                "INSERT INTO content_blobs (hash, language, byte_len, line_count) ",
            );
            qb.push_values(chunk.iter().copied(), |mut b, blob| {
                b.push_bind(&blob.hash)
                    .push_bind(&blob.language)
                    .push_bind(blob.byte_len)
                    .push_bind(blob.line_count);
            });
            qb.push(
                " ON CONFLICT (hash) DO UPDATE SET language = EXCLUDED.language, byte_len = EXCLUDED.byte_len, line_count = EXCLUDED.line_count",
            );

            qb.build()
                .execute(tx.as_mut())
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }

    async fn insert_file_pointers(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        files: &[pointer_indexer::models::FilePointer],
    ) -> Result<(), DbError> {
        if files.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(files, |file| {
            (
                file.repository.clone(),
                file.commit_sha.clone(),
                file.file_path.clone(),
            )
        });

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new(
                "INSERT INTO files (repository, commit_sha, file_path, content_hash) ",
            );
            qb.push_values(chunk.iter().copied(), |mut b, file| {
                b.push_bind(&file.repository)
                    .push_bind(&file.commit_sha)
                    .push_bind(&file.file_path)
                    .push_bind(&file.content_hash);
            });
            qb.push(
                " ON CONFLICT (repository, commit_sha, file_path) DO UPDATE SET content_hash = EXCLUDED.content_hash",
            );

            qb.build()
                .execute(tx.as_mut())
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }

    async fn insert_symbol_records(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        symbols: &[pointer_indexer::models::SymbolRecord],
    ) -> Result<(), DbError> {
        if symbols.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(symbols, |symbol| {
            (
                symbol.content_hash.clone(),
                symbol.namespace.clone(),
                symbol.symbol.clone(),
                symbol.kind.clone(),
            )
        });

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new(
                "INSERT INTO symbols (content_hash, namespace, symbol, fully_qualified, kind) ",
            );
            qb.push_values(chunk.iter().copied(), |mut b, symbol| {
                b.push_bind(&symbol.content_hash)
                    .push_bind(&symbol.namespace)
                    .push_bind(&symbol.symbol)
                    .push_bind(&symbol.fully_qualified)
                    .push_bind(&symbol.kind);
            });
            qb.push(
                " ON CONFLICT (content_hash, namespace, symbol, kind) DO UPDATE SET fully_qualified = EXCLUDED.fully_qualified",
            );

            qb.build()
                .execute(tx.as_mut())
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }

    async fn insert_reference_records(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        references: &[pointer_indexer::models::ReferenceRecord],
    ) -> Result<(), DbError> {
        if references.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(references, |reference| {
            (
                reference.content_hash.clone(),
                reference.namespace.clone(),
                reference.name.clone(),
                reference.kind.clone(),
                reference.line,
                reference.column,
            )
        });

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new(
                "INSERT INTO symbol_references (content_hash, namespace, name, fully_qualified, kind, line_number, column_number) ",
            );
            qb.push_values(chunk.iter().copied(), |mut b, reference| {
                let line: i32 = reference.line.try_into().unwrap_or(i32::MAX);
                let column: i32 = reference.column.try_into().unwrap_or(i32::MAX);
                b.push_bind(&reference.content_hash)
                    .push_bind(&reference.namespace)
                    .push_bind(&reference.name)
                    .push_bind(&reference.fully_qualified)
                    .push_bind(&reference.kind)
                    .push_bind(line)
                    .push_bind(column);
            });
            qb.push(
                " ON CONFLICT (content_hash, namespace, name, line_number, column_number, kind) DO NOTHING",
            );

            qb.build()
                .execute(tx.as_mut())
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }

    async fn insert_file_chunk_records(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        records: &[pointer_indexer::models::FileChunkRecord],
    ) -> Result<(), DbError> {
        if records.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(records, |record| {
            (
                record.repository.clone(),
                record.commit_sha.clone(),
                record.file_path.clone(),
                record.sequence,
            )
        });

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new(
                "INSERT INTO file_chunks (repository, commit_sha, file_path, chunk_order, chunk_hash, byte_offset, byte_len, start_line, line_count) ",
            );
            qb.push_values(chunk.iter().copied(), |mut b, record| {
                let chunk_order: i32 = record.sequence.try_into().unwrap_or(i32::MAX);
                let byte_offset: i64 = record.byte_offset.try_into().unwrap_or(i64::MAX);
                let byte_len: i32 = record.byte_len.try_into().unwrap_or(i32::MAX);
                let start_line: i32 = record.start_line.try_into().unwrap_or(i32::MAX);
                let line_count: i32 = record.line_count.try_into().unwrap_or(i32::MAX);
                b.push_bind(&record.repository)
                    .push_bind(&record.commit_sha)
                    .push_bind(&record.file_path)
                    .push_bind(chunk_order)
                    .push_bind(&record.chunk_hash)
                    .push_bind(byte_offset)
                    .push_bind(byte_len)
                    .push_bind(start_line)
                    .push_bind(line_count);
            });
            qb.push(
                " ON CONFLICT (repository, commit_sha, file_path, chunk_order) DO UPDATE SET chunk_hash = EXCLUDED.chunk_hash, byte_offset = EXCLUDED.byte_offset, byte_len = EXCLUDED.byte_len, start_line = EXCLUDED.start_line, line_count = EXCLUDED.line_count",
            );

            qb.build()
                .execute(tx.as_mut())
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;
        }

        Ok(())
    }
}

const INSERT_BATCH_SIZE: usize = 1000;

#[derive(sqlx::FromRow)]
struct UploadChunkRow {
    chunk_index: i32,
    total_chunks: i32,
    data: Vec<u8>,
}

struct FileData {
    bytes: Vec<u8>,
    language: Option<String>,
}

#[derive(sqlx::FromRow)]
struct FileChunkDataRow {
    chunk_order: i32,
    chunk_hash: String,
    byte_len: i32,
}

#[derive(sqlx::FromRow)]
struct SymbolRow {
    symbol: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    language: Option<String>,
    repository: String,
    commit_sha: String,
    file_path: String,
}

#[derive(sqlx::FromRow)]
struct ReferenceRow {
    fully_qualified: String,
    name: String,
    namespace: Option<String>,
    kind: Option<String>,
    line: i32,
    column: i32,
}

struct DecodedChunk {
    hash: String,
    algorithm: String,
    byte_len: u32,
    data: Vec<u8>,
}

fn dedup_by_key<'a, T, K, F>(items: &'a [T], mut key: F) -> Vec<&'a T>
where
    K: Eq + std::hash::Hash,
    F: FnMut(&'a T) -> K,
{
    let mut seen = std::collections::HashSet::new();
    let mut deduped = Vec::with_capacity(items.len());

    for item in items {
        if seen.insert(key(item)) {
            deduped.push(item);
        }
    }

    deduped
}
