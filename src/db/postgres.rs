use crate::db::models::{FileReference as DbFileReference, SearchResultsPage, SearchSnippet};
use crate::db::{
    Database, DbError, DbUniqueChunk, FileReference, RawFileContent, ReferenceResult, RepoSummary,
    RepoTreeQuery, SearchRequest, SearchResponse, SearchResult, SnippetRequest, SnippetResponse,
    SymbolReferenceRequest, SymbolReferenceResponse, SymbolResult, TreeEntry, TreeResponse,
};
use crate::dsl::{CaseSensitivity, ContentPredicate, TextSearchPlan, TextSearchRequest};
use async_trait::async_trait;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use std::{
    collections::{HashMap, HashSet},
    io::Read,
};

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
        let branches: Vec<String> =
            sqlx::query_scalar("SELECT branch FROM branches WHERE repository = $1 ORDER BY branch")
                .bind(repository)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

        if branches.is_empty() {
            let commits: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 ORDER BY commit_sha DESC",
            )
            .bind(repository)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

            Ok(commits)
        } else {
            Ok(branches)
        }
    }

    async fn resolve_branch_head(
        &self,
        repository: &str,
        branch: &str,
    ) -> Result<Option<String>, DbError> {
        let commit: Option<String> = sqlx::query_scalar(
            "SELECT commit_sha FROM branches WHERE repository = $1 AND branch = $2",
        )
        .bind(repository)
        .bind(branch)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(commit)
    }

    async fn chunk_need(&self, hashes: Vec<String>) -> Result<Vec<String>, DbError> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let existing: Vec<(String,)> =
            sqlx::query_as("SELECT chunk_hash FROM chunks WHERE chunk_hash = ANY($1)")
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

    async fn chunk_upload(&self, chunks: Vec<DbUniqueChunk>) -> Result<(), DbError> {
        if chunks.is_empty() {
            return Ok(());
        }

        for batch in chunks.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new("INSERT INTO chunks (chunk_hash, text_content) ");
            qb.push_values(batch, |mut b, chunk| {
                b.push_bind(chunk.chunk_hash.clone())
                    .push_bind(chunk.text_content.clone());
            });
            qb.push(" ON CONFLICT (chunk_hash) DO NOTHING");

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
    ) -> Result<RawFileContent, DbError> {
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
        Ok(RawFileContent {
            repository: repository.to_string(),
            commit_sha: commit_sha.to_string(),
            file_path: file_path.to_string(),
            language: data.language,
            content: text,
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
            "SELECT s.symbol, s.namespace, s.kind, s.fully_qualified, cb.language, \
                    f.repository, f.commit_sha, f.file_path, def.line_number AS line, def.column_number AS column \
             FROM symbols s \
             JOIN content_blobs cb ON cb.hash = s.content_hash \
             JOIN files f ON f.content_hash = s.content_hash \
             LEFT JOIN LATERAL ( \
                 SELECT line_number, column_number \
                 FROM symbol_references sr \
                 WHERE sr.content_hash = s.content_hash \
                   AND sr.name = s.symbol \
                   AND sr.namespace IS NOT DISTINCT FROM s.namespace \
                 ORDER BY line_number ASC, column_number ASC \
                 LIMIT 1 \
             ) def ON TRUE",
        );

        if let Some(q) = &request.q {
            let matching_hashes: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT cbc.content_hash \
                 FROM chunks c \
                 JOIN content_blob_chunks cbc ON c.chunk_hash = cbc.chunk_hash \
                 WHERE c.text_content LIKE '%' || $1 || '%'",
            )
            .bind(q)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

            if matching_hashes.is_empty() {
                return Ok(SearchResponse {
                    symbols: Vec::new(),
                });
            }

            qb.push(" WHERE s.content_hash = ANY(")
                .push_bind(matching_hashes)
                .push(")");
        } else {
            qb.push(" WHERE 1=1");
        }

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
            if !kinds.is_empty() {
                qb.push(" AND s.kind = ANY(").push_bind(kinds).push(")");
            }
        }

        if let Some(languages) = &request.language {
            if !languages.is_empty() {
                qb.push(" AND cb.language = ANY(")
                    .push_bind(languages)
                    .push(")");
            }
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
        let mut reference_map: HashMap<String, Vec<ReferenceRow>> = HashMap::new();

        if include_refs {
            let fully_qualified: HashSet<String> =
                rows.iter().map(|row| row.fully_qualified.clone()).collect();

            if !fully_qualified.is_empty() {
                let lookup: Vec<String> = fully_qualified.into_iter().collect();
                let ref_rows: Vec<ReferenceRow> = sqlx::query_as(
                    "SELECT fully_qualified, name, namespace, kind, \
                            line_number AS line, column_number AS column \
                     FROM symbol_references \
                     WHERE fully_qualified = ANY($1)",
                )
                .bind(&lookup)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

                for reference in ref_rows {
                    reference_map
                        .entry(reference.fully_qualified.clone())
                        .or_default()
                        .push(reference);
                }
            }
        }

        let needle_lower = request.name.as_ref().map(|s| s.to_lowercase());
        let needle_lower_ref = needle_lower.as_deref();

        let mut scored_rows: Vec<(f32, SymbolRow)> = rows
            .into_iter()
            .map(|row| (score_symbol_row(&row, needle_lower_ref), row))
            .collect();

        scored_rows.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.symbol.cmp(&b.1.symbol))
        });

        let mut results = Vec::with_capacity(scored_rows.len());
        for (_, row) in scored_rows {
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

            let line = row
                .line
                .and_then(|line| line.try_into().ok())
                .and_then(|line: i32| (line > 0).then(|| line as usize));
            let column = row
                .column
                .and_then(|column| column.try_into().ok())
                .and_then(|column: i32| (column > 0).then(|| column as usize));

            results.push(SymbolResult {
                symbol: row.symbol,
                namespace: row.namespace,
                kind: row.kind,
                fully_qualified: row.fully_qualified,
                repository: row.repository,
                commit_sha: row.commit_sha,
                file_path: row.file_path,
                language: row.language,
                line,
                column,
                references,
            });
        }

        Ok(SearchResponse { symbols: results })
    }

    async fn text_search(&self, request: &TextSearchRequest) -> Result<SearchResultsPage, DbError> {
        fn push_content_condition(
            qb: &mut QueryBuilder<'_, Postgres>,
            predicate: &ContentPredicate,
            case_mode: CaseSensitivity,
            negate: bool,
        ) {
            let (like_op, regex_op) = match case_mode {
                CaseSensitivity::Yes => (" LIKE ", " ~ "),
                _ => (" ILIKE ", " ~* "),
            };

            qb.push(" AND ");
            if negate {
                qb.push("NOT (");
            } else {
                qb.push("(");
            }

            match predicate {
                ContentPredicate::Plain(value) => {
                    qb.push("c.text_content");
                    qb.push(like_op);
                    qb.push("'%' || ");
                    qb.push_bind(value.clone());
                    qb.push(" || '%'");
                }
                ContentPredicate::Regex(pattern) => {
                    qb.push("c.text_content");
                    qb.push(regex_op);
                    qb.push_bind(pattern.clone());
                }
            }

            qb.push(")");
        }

        fn has_uppercase(value: &str) -> bool {
            value.chars().any(|ch| ch.is_ascii_uppercase())
        }

        fn resolve_case(plan: &TextSearchPlan) -> CaseSensitivity {
            match plan.case_sensitivity {
                Some(CaseSensitivity::Yes) => CaseSensitivity::Yes,
                Some(CaseSensitivity::No) => CaseSensitivity::No,
                Some(CaseSensitivity::Auto) => {
                    let any_upper = plan
                        .required_terms
                        .iter()
                        .filter_map(|term| match term {
                            ContentPredicate::Plain(value) => Some(value),
                            _ => None,
                        })
                        .any(|value| has_uppercase(value));
                    if any_upper {
                        CaseSensitivity::Yes
                    } else {
                        CaseSensitivity::No
                    }
                }
                None => CaseSensitivity::No,
            }
        }

        if request.plans.is_empty() {
            return Ok(SearchResultsPage::empty(
                request.original_query.clone(),
                request.page,
                request.page_size,
            ));
        }

        let mut qb = QueryBuilder::new("WITH plan_results AS (");

        for (idx, plan) in request.plans.iter().enumerate() {
            if idx > 0 {
                qb.push(" UNION ALL ");
            }

            let case_mode = resolve_case(plan);
            let highlight_case_sensitive = matches!(case_mode, CaseSensitivity::Yes);

            qb.push("(");
            qb.push(
                "
                SELECT
                    cm.repository,
                    cm.commit_sha,
                    cm.file_path,
                    cm.content_hash,
                    cm.start_line,
                    cm.chunk_line_count AS line_count,
                    ctx.context_snippet AS content_text,
                    ctx.match_line_number,
                ",
            );
            qb.push_bind(plan.include_historical);
            qb.push(
                " AS include_historical
                FROM (
                    SELECT
                        f.repository,
                        f.commit_sha,
                        f.file_path,
                        cbc.content_hash,
                        cbc.chunk_index,
                        cbc.chunk_line_count,
                        c.text_content,
                        1 + COALESCE(
                            SUM(
                                cbc.chunk_line_count
                                - CASE
                                    WHEN RIGHT(c.text_content, 1) = E'\n' OR c.text_content = '' THEN 0
                                    ELSE 1
                                  END
                            ) OVER (
                                PARTITION BY cbc.content_hash
                                ORDER BY cbc.chunk_index
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ),
                            0
                        ) AS start_line
                    FROM
                        chunks c
                    JOIN
                        content_blob_chunks cbc ON c.chunk_hash = cbc.chunk_hash
                    JOIN
                        files f ON cbc.content_hash = f.content_hash
                    JOIN
                        content_blobs cb ON cb.hash = f.content_hash
                    WHERE
                        TRUE",
            );

            for predicate in &plan.required_terms {
                push_content_condition(&mut qb, predicate, case_mode, false);
            }

            for predicate in &plan.excluded_terms {
                push_content_condition(&mut qb, predicate, case_mode, true);
            }

            if !plan.repos.is_empty() {
                qb.push(" AND f.repository = ANY(");
                qb.push_bind(&plan.repos);
                qb.push(")");
            }

            if !plan.excluded_repos.is_empty() {
                qb.push(" AND NOT (f.repository = ANY(");
                qb.push_bind(&plan.excluded_repos);
                qb.push("))");
            }

            if !plan.file_globs.is_empty() {
                for pattern in &plan.file_globs {
                    qb.push(" AND f.file_path ILIKE ");
                    qb.push_bind(pattern);
                    qb.push(" ESCAPE '\\\\'");
                }
            }

            if !plan.excluded_file_globs.is_empty() {
                for pattern in &plan.excluded_file_globs {
                    qb.push(" AND f.file_path NOT ILIKE ");
                    qb.push_bind(pattern);
                    qb.push(" ESCAPE '\\\\'");
                }
            }

            if !plan.langs.is_empty() {
                qb.push(" AND cb.language = ANY(");
                qb.push_bind(&plan.langs);
                qb.push(")");
            }

            if !plan.excluded_langs.is_empty() {
                qb.push(" AND NOT (cb.language = ANY(");
                qb.push_bind(&plan.excluded_langs);
                qb.push("))");
            }

            if !plan.branches.is_empty() {
                qb.push(" AND f.commit_sha = ANY(");
                qb.push_bind(&plan.branches);
                qb.push(")");
            }

            if !plan.excluded_branches.is_empty() {
                qb.push(" AND NOT (f.commit_sha = ANY(");
                qb.push_bind(&plan.excluded_branches);
                qb.push("))");
            }

            qb.push(
                "
                ) cm
                CROSS JOIN LATERAL extract_context_with_highlight(cm.text_content, ",
            );
            qb.push_bind(&plan.highlight_pattern);
            qb.push(", 3, ");
            qb.push_bind(highlight_case_sensitive);
            qb.push(") ctx)");
        }

        let page_index = u64::from(request.page);
        let page_size = u64::from(request.page_size.max(1));
        let sample_factor = u64::from(FILE_SAMPLE_FACTOR.max(1));
        let base_limit = page_index
            .saturating_add(1)
            .saturating_mul(page_size)
            .saturating_mul(sample_factor);
        let minimum = page_size.saturating_mul(sample_factor);
        let fetch_limit_u64 = base_limit.max(minimum).saturating_add(1);
        let fetch_limit = fetch_limit_u64.min(i64::MAX as u64) as i64;

        qb.push(
            ")
            SELECT DISTINCT ON (pr.repository, pr.commit_sha, pr.file_path, pr.start_line, pr.match_line_number)
                pr.repository,
                pr.commit_sha,
                pr.file_path,
                pr.content_hash,
                pr.start_line,
                pr.line_count,
                pr.content_text,
                pr.match_line_number,
                COALESCE(branch_match.branches, ARRAY[]::TEXT[]) AS branches,
                CASE
                    WHEN repo_branches.repo_has_branches IS TRUE AND branch_match.branches IS NULL THEN TRUE
                    ELSE FALSE
                END AS is_historical
            FROM
                plan_results pr
            LEFT JOIN LATERAL (
                SELECT array_agg(DISTINCT branch) AS branches
                FROM branches b
                WHERE b.repository = pr.repository AND b.commit_sha = pr.commit_sha
            ) branch_match ON TRUE
            LEFT JOIN LATERAL (
                SELECT TRUE AS repo_has_branches
                FROM branches b
                WHERE b.repository = pr.repository
                LIMIT 1
            ) repo_branches ON TRUE
            WHERE
                pr.include_historical
                OR branch_match.branches IS NOT NULL
                OR repo_branches.repo_has_branches IS NULL
            ORDER BY
                pr.repository,
                pr.commit_sha,
                pr.file_path,
                pr.start_line,
                pr.match_line_number
            LIMIT ",
        );
        qb.push_bind(fetch_limit);

        let query = qb.build_query_as::<SearchResultRow>();
        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        let row_limit_hit = (rows.len() as i64) >= fetch_limit;

        if rows.is_empty() {
            return Ok(SearchResultsPage::empty(
                request.original_query.clone(),
                request.page,
                request.page_size,
            ));
        }

        let plain_terms = collect_plain_terms(request);
        let symbol_terms: HashSet<String> = plain_terms
            .iter()
            .filter(|term| looks_like_symbol(term))
            .cloned()
            .collect();
        let symbol_terms_lower: HashSet<String> = symbol_terms
            .iter()
            .map(|term| term.to_lowercase())
            .collect();

        let mut grouped: HashMap<FileGroupKey, Vec<(SearchResultRow, SnippetAnalysis)>> =
            HashMap::new();
        for row in rows.into_iter() {
            let analysis = analyze_snippet(&row.content_text, &symbol_terms, &symbol_terms_lower);
            let key = FileGroupKey {
                repository: row.repository.clone(),
                commit_sha: row.commit_sha.clone(),
                file_path: row.file_path.clone(),
                content_hash: row.content_hash.clone(),
            };
            grouped.entry(key).or_default().push((row, analysis));
        }

        let content_hashes: Vec<String> = grouped
            .keys()
            .map(|key| key.content_hash.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let symbol_terms_vec: Vec<String> = symbol_terms.iter().cloned().collect();
        let symbol_terms_lower_vec: Vec<String> = symbol_terms_lower.iter().cloned().collect();

        let symbol_meta = load_symbol_metadata(
            &self.pool,
            &content_hashes,
            &symbol_terms_vec,
            &symbol_terms_lower_vec,
        )
        .await?;

        let mut aggregates = Vec::with_capacity(grouped.len());
        for (key, entries) in grouped {
            let meta = symbol_meta
                .get(&key.content_hash)
                .cloned()
                .unwrap_or_default();
            let classification = determine_match_class(&meta);
            let base_score = classification.base_score();

            let mut scored_entries: Vec<(SearchResultRow, f32)> = Vec::with_capacity(entries.len());
            let mut best_index: Option<usize> = None;
            let mut best_score = f32::NEG_INFINITY;

            for (row, analysis) in entries {
                let mut score = base_score;
                if meta.exact_symbol_match {
                    score += 30.0;
                } else if analysis.matches_symbol_candidate {
                    score += 15.0;
                }
                if analysis.has_full_match {
                    score += 8.0;
                }
                if analysis.has_partial_match && !analysis.has_full_match {
                    score -= 8.0;
                }
                let match_line = row.match_line_number.max(1) as f32;
                score += 5.0_f32 / match_line;

                let idx = scored_entries.len();
                scored_entries.push((row, score));

                if score > best_score {
                    best_score = score;
                    best_index = Some(idx);
                } else if (score - best_score).abs() < f32::EPSILON {
                    if let Some(current_idx) = best_index {
                        let current_row = &scored_entries[current_idx].0;
                        let candidate_row = &scored_entries[idx].0;
                        if candidate_row.match_line_number < current_row.match_line_number
                            || (candidate_row.match_line_number == current_row.match_line_number
                                && candidate_row.file_path < current_row.file_path)
                        {
                            best_index = Some(idx);
                        }
                    }
                }
            }

            if let Some(best_idx) = best_index {
                let (best_row, best_score) = scored_entries.swap_remove(best_idx);
                let mut ordered_rows: Vec<SearchResultRow> =
                    Vec::with_capacity(scored_entries.len() + 1);
                ordered_rows.push(best_row);
                scored_entries.sort_by(|a, b| {
                    a.0.match_line_number
                        .cmp(&b.0.match_line_number)
                        .then_with(|| a.0.file_path.cmp(&b.0.file_path))
                });
                ordered_rows.extend(scored_entries.into_iter().map(|(row, _)| row));

                aggregates.push(FileAggregate {
                    entries: ordered_rows,
                    classification,
                    score: best_score,
                });
            }
        }

        aggregates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.classification.cmp(&b.classification))
                .then_with(|| {
                    let a_best = &a.entries[0];
                    let b_best = &b.entries[0];
                    a_best
                        .match_line_number
                        .cmp(&b_best.match_line_number)
                        .then_with(|| a_best.file_path.cmp(&b_best.file_path))
                })
        });

        let total = aggregates.len();
        let page_index = request.page.saturating_sub(1) as usize;
        let page_size = request.page_size as usize;
        let start = page_index.saturating_mul(page_size);
        let mut has_more = total > start + page_size;
        if !has_more && total > 0 && row_limit_hit {
            has_more = true;
        }

        let results = if start >= total {
            Vec::new()
        } else {
            aggregates
                .into_iter()
                .skip(start)
                .take(page_size)
                .map(|agg| {
                    let mut entries_iter = agg.entries.into_iter();
                    let best_row = entries_iter
                        .next()
                        .expect("aggregated results should contain at least one snippet");

                    let best_start_line: i32 = best_row.start_line.try_into().unwrap_or(i32::MAX);
                    let best_match_line =
                        best_start_line.saturating_add(best_row.match_line_number - 1);
                    let best_end_line =
                        best_start_line.saturating_add(best_row.line_count.saturating_sub(1));

                    let mut snippets = Vec::new();
                    snippets.push(SearchSnippet {
                        start_line: best_start_line,
                        end_line: best_end_line,
                        match_line: best_match_line,
                        content_text: best_row.content_text.clone(),
                    });

                    for row in entries_iter {
                        let snippet_start: i32 = row.start_line.try_into().unwrap_or(i32::MAX);
                        let snippet_match = snippet_start.saturating_add(row.match_line_number - 1);
                        let snippet_end =
                            snippet_start.saturating_add(row.line_count.saturating_sub(1));
                        snippets.push(SearchSnippet {
                            start_line: snippet_start,
                            end_line: snippet_end,
                            match_line: snippet_match,
                            content_text: row.content_text,
                        });
                    }

                    SearchResult {
                        repository: best_row.repository,
                        commit_sha: best_row.commit_sha,
                        file_path: best_row.file_path,
                        start_line: best_start_line,
                        end_line: best_end_line,
                        match_line: best_match_line,
                        content_text: best_row.content_text,
                        snippets,
                        branches: best_row.branches,
                        is_historical: best_row.is_historical,
                    }
                })
                .collect()
        };

        Ok(SearchResultsPage {
            results,
            has_more,
            page: request.page,
            page_size: request.page_size,
            query: request.original_query.clone(),
        })
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
        let row: (String, Option<String>) = sqlx::query_as(
            "SELECT f.content_hash, cb.language
             FROM files f
             JOIN content_blobs cb ON cb.hash = f.content_hash
             WHERE f.repository = $1 AND f.commit_sha = $2 AND f.file_path = $3",
        )
        .bind(repository)
        .bind(commit_sha)
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?
        .ok_or_else(|| DbError::Internal("file not found".to_string()))?;

        let (content_hash, language) = row;

        let chunk_rows: Vec<(String,)> = sqlx::query_as(
            "SELECT c.text_content
             FROM content_blob_chunks cbc
             JOIN chunks c ON cbc.chunk_hash = c.chunk_hash
             WHERE cbc.content_hash = $1
             ORDER BY cbc.chunk_index",
        )
        .bind(&content_hash)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if chunk_rows.is_empty() {
            // This could happen for binary files or empty files
            return Ok(FileData {
                bytes: Vec::new(),
                language,
            });
        }

        let bytes = chunk_rows
            .into_iter()
            .map(|s| s.0)
            .flat_map(|v| v.into_bytes().into_iter())
            .collect();

        Ok(FileData { bytes, language })
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
        self.upsert_branch_heads(&mut tx, &report.branches).await?;

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

    async fn upsert_branch_heads(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        branches: &[pointer_indexer::models::BranchHead],
    ) -> Result<(), DbError> {
        if branches.is_empty() {
            return Ok(());
        }

        let deduped = dedup_by_key(branches, |branch| {
            (branch.repository.clone(), branch.branch.clone())
        });

        let mut qb = QueryBuilder::new("INSERT INTO branches (repository, branch, commit_sha) ");
        qb.push_values(deduped.into_iter(), |mut b, branch| {
            b.push_bind(&branch.repository)
                .push_bind(&branch.branch)
                .push_bind(&branch.commit_sha);
        });
        qb.push(
            " ON CONFLICT (repository, branch)
              DO UPDATE SET commit_sha = EXCLUDED.commit_sha, indexed_at = NOW()",
        );

        qb.build()
            .execute(tx.as_mut())
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(())
    }
}

const FILE_SAMPLE_FACTOR: u32 = 6;
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

#[derive(sqlx::FromRow, Debug, Clone)]
struct SearchResultRow {
    repository: String,
    commit_sha: String,
    file_path: String,
    content_hash: String,
    start_line: i64,
    line_count: i32,
    content_text: String,
    match_line_number: i32,
    branches: Vec<String>,
    is_historical: bool,
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
    line: Option<i32>,
    column: Option<i32>,
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

#[derive(Clone, Debug)]
struct FileAggregate {
    entries: Vec<SearchResultRow>,
    classification: MatchClass,
    score: f32,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MatchClass {
    Definition,
    Declaration,
    Reference,
    Other,
}

impl MatchClass {
    fn base_score(self) -> f32 {
        match self {
            MatchClass::Definition => 100.0,
            MatchClass::Declaration => 80.0,
            MatchClass::Reference => 55.0,
            MatchClass::Other => 25.0,
        }
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct FileGroupKey {
    repository: String,
    commit_sha: String,
    file_path: String,
    content_hash: String,
}

#[derive(Clone, Debug, Default)]
struct SnippetAnalysis {
    has_full_match: bool,
    has_partial_match: bool,
    matches_symbol_candidate: bool,
}

#[derive(Clone, Debug, Default)]
struct FileSymbolMeta {
    best_role: Option<MatchClass>,
    has_reference: bool,
    exact_symbol_match: bool,
}

fn determine_match_class(meta: &FileSymbolMeta) -> MatchClass {
    if let Some(role) = meta.best_role {
        role
    } else if meta.has_reference {
        MatchClass::Reference
    } else {
        MatchClass::Other
    }
}

fn collect_plain_terms(request: &TextSearchRequest) -> HashSet<String> {
    let mut terms = HashSet::new();
    for plan in &request.plans {
        for predicate in &plan.required_terms {
            if let ContentPredicate::Plain(value) = predicate {
                if !value.is_empty() {
                    terms.insert(value.clone());
                }
            }
        }
    }
    terms
}

fn looks_like_symbol(term: &str) -> bool {
    if term.is_empty() || term.len() > 128 {
        return false;
    }
    let mut has_alpha = false;
    for ch in term.chars() {
        if !is_symbol_char(ch) {
            return false;
        }
        if ch.is_ascii_alphabetic() || ch == '_' {
            has_alpha = true;
        }
    }
    has_alpha
}

fn is_symbol_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '_' | ':' | '.' | '$' | '#')
}

fn analyze_snippet(
    snippet: &str,
    symbol_terms: &HashSet<String>,
    symbol_terms_lower: &HashSet<String>,
) -> SnippetAnalysis {
    let mut analysis = SnippetAnalysis::default();
    let mut cursor = 0usize;

    while let Some(start) = snippet[cursor..].find("<mark>") {
        let mark_open = cursor + start;
        let mark_start = mark_open + "<mark>".len();
        if let Some(end_rel) = snippet[mark_start..].find("</mark>") {
            let mark_end = mark_start + end_rel;
            let matched = &snippet[mark_start..mark_end];
            let matched_trimmed = matched.trim();
            if !matched_trimmed.is_empty() {
                let matched_lower = matched_trimmed.to_lowercase();
                if symbol_terms.contains(matched_trimmed)
                    || symbol_terms_lower.contains(&matched_lower)
                {
                    analysis.matches_symbol_candidate = true;
                }

                let before_char = snippet[..mark_open].chars().rev().find(|c| !c.is_control());
                let after_index = mark_end + "</mark>".len();
                let after_char = snippet[after_index..].chars().find(|c| !c.is_control());

                let before_is_symbol = before_char.map_or(false, is_symbol_char);
                let after_is_symbol = after_char.map_or(false, is_symbol_char);

                if !before_is_symbol && !after_is_symbol {
                    analysis.has_full_match = true;
                } else {
                    analysis.has_partial_match = true;
                }
            }
            cursor = mark_end + "</mark>".len();
        } else {
            break;
        }
    }

    analysis
}

async fn load_symbol_metadata(
    pool: &PgPool,
    content_hashes: &[String],
    symbol_terms: &[String],
    symbol_terms_lower: &[String],
) -> Result<HashMap<String, FileSymbolMeta>, DbError> {
    let mut meta_map: HashMap<String, FileSymbolMeta> = HashMap::new();

    if content_hashes.is_empty() {
        return Ok(meta_map);
    }

    if !symbol_terms.is_empty() {
        let symbol_rows: Vec<(String, Option<String>, String, String)> = sqlx::query_as(
            "SELECT content_hash, kind, symbol, fully_qualified
             FROM symbols
             WHERE content_hash = ANY($1)
               AND (
                    symbol = ANY($2)
                    OR fully_qualified = ANY($2)
                    OR LOWER(symbol) = ANY($3)
                    OR LOWER(fully_qualified) = ANY($3)
               )",
        )
        .bind(content_hashes)
        .bind(symbol_terms)
        .bind(symbol_terms_lower)
        .fetch_all(pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        for (content_hash, kind, symbol, fully_qualified) in symbol_rows {
            let entry = meta_map.entry(content_hash.clone()).or_default();
            if let Some(kind) = kind.as_deref() {
                let role = match kind {
                    "definition" => Some(MatchClass::Definition),
                    "declaration" => Some(MatchClass::Declaration),
                    _ => None,
                };
                if let Some(role) = role {
                    if entry
                        .best_role
                        .map(|current| role < current)
                        .unwrap_or(true)
                    {
                        entry.best_role = Some(role);
                    }
                }
            }

            if symbol_terms.contains(&symbol)
                || symbol_terms.contains(&fully_qualified)
                || symbol_terms_lower.contains(&symbol.to_lowercase())
                || symbol_terms_lower.contains(&fully_qualified.to_lowercase())
            {
                entry.exact_symbol_match = true;
            }
        }
    }

    if !symbol_terms.is_empty() {
        let reference_rows: Vec<(String, Option<String>, String, String)> = sqlx::query_as(
            "SELECT content_hash, kind, name, fully_qualified
             FROM symbol_references
             WHERE content_hash = ANY($1)
               AND (
                    name = ANY($2)
                    OR fully_qualified = ANY($2)
                    OR LOWER(name) = ANY($3)
                    OR LOWER(fully_qualified) = ANY($3)
               )",
        )
        .bind(content_hashes)
        .bind(symbol_terms)
        .bind(symbol_terms_lower)
        .fetch_all(pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        for (content_hash, kind, name, fully_qualified) in reference_rows {
            let entry = meta_map.entry(content_hash.clone()).or_default();
            if kind.as_deref() == Some("reference") {
                entry.has_reference = true;
            } else {
                entry.has_reference = true;
            }
            if symbol_terms.contains(&name)
                || symbol_terms.contains(&fully_qualified)
                || symbol_terms_lower.contains(&name.to_lowercase())
                || symbol_terms_lower.contains(&fully_qualified.to_lowercase())
            {
                entry.exact_symbol_match = true;
            }
        }
    }

    Ok(meta_map)
}

fn score_symbol_row(row: &SymbolRow, needle_lower: Option<&str>) -> f32 {
    let mut score = match row.kind.as_deref() {
        Some("definition") => 120.0,
        Some("declaration") => 90.0,
        _ => 50.0,
    };

    if let Some(needle) = needle_lower {
        let symbol_lower = row.symbol.to_lowercase();
        if symbol_lower == needle {
            score += 40.0;
        } else if symbol_lower.contains(needle) {
            score += 15.0;
        }

        let fq_lower = row.fully_qualified.to_lowercase();
        if fq_lower == needle {
            score += 35.0;
        } else if fq_lower.contains(needle) {
            score += 12.0;
        }
    }

    score
}
