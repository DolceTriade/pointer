use crate::db::models::{
    FacetCount, FileReference as DbFileReference, RepoBranchInfo, SearchResultsPage,
    SearchResultsStats, SearchSnippet, SymbolSuggestion,
};
use crate::db::{
    Database, DbError, DbUniqueChunk, FileReference, RawFileContent, ReferenceResult, RepoSummary,
    RepoTreeQuery, SearchRequest, SearchResponse, SearchResult, SnippetRequest, SnippetResponse,
    SymbolReferenceRequest, SymbolReferenceResponse, SymbolResult, TreeEntry, TreeResponse,
};
use crate::dsl::{
    CaseSensitivity, ContentPredicate, TextSearchPlan, TextSearchRequest, escape_sql_like_literal,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgArguments;
use sqlx::{Execute, PgPool, Postgres, QueryBuilder, Transaction, types::Json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
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

fn push_content_predicate(
    qb: &mut QueryBuilder<'_, Postgres>,
    predicate: &ContentPredicate,
    case_mode: CaseSensitivity,
    column: &str,
) {
    let (like_op, regex_op) = match case_mode {
        CaseSensitivity::Yes => (" LIKE ", " ~ "),
        _ => (" ILIKE ", " ~* "),
    };

    match predicate {
        ContentPredicate::Plain(value) => {
            let escaped = escape_sql_like_literal(value);
            qb.push(column);
            qb.push(like_op);
            qb.push("'%' || ");
            qb.push_bind(escaped);
            qb.push(" || '%' ESCAPE '\\'");
        }
        ContentPredicate::Regex(pattern) => {
            qb.push(column);
            qb.push(regex_op);
            qb.push_bind(pattern.clone());
        }
    }
}

fn push_content_condition(
    qb: &mut QueryBuilder<'_, Postgres>,
    predicate: &ContentPredicate,
    case_mode: CaseSensitivity,
    negate: bool,
) {
    qb.push(" AND ");
    if negate {
        qb.push("NOT (");
    } else {
        qb.push("(");
    }

    push_content_predicate(qb, predicate, case_mode, "c.text_content");

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

fn push_search_ctes<'a>(
    qb: &mut QueryBuilder<'a, Postgres>,
    request: &'a TextSearchRequest,
    plan_row_limit: i64,
    fetch_limit: i64,
    file_limit: i64,
    needs_live_branch_filter: bool,
    symbol_terms: &'a [String],
) {
    qb.push("WITH ");

    if needs_live_branch_filter {
        qb.push(
            "
                live_repos AS (
                    SELECT DISTINCT repository
                    FROM repo_live_branches
                    UNION
                    SELECT DISTINCT repository
                    FROM branches
                ),
                live_commits AS (
                    SELECT DISTINCT b.repository, b.commit_sha
                    FROM repo_live_branches lb
                    JOIN branches b
                      ON b.repository = lb.repository
                     AND b.branch = lb.branch
                    UNION
                    SELECT DISTINCT b.repository, b.commit_sha
                    FROM branches b
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM repo_live_branches lb
                        WHERE lb.repository = b.repository
                    )
                ),",
        );
    }

    qb.push("plan_results AS (");

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
                    files.id AS file_id,
                    files.content_hash,
                    cbc.chunk_line_count AS line_count,
                    cbc.chunk_index,
                ",
        );
        qb.push_bind(&plan.highlight_pattern);
        qb.push(
            " AS highlight_pattern,
                ",
        );
        qb.push_bind(highlight_case_sensitive);
        qb.push(
            " AS highlight_case_sensitive,
                ",
        );
        qb.push_bind(plan.include_historical);
        qb.push(
            " AS include_historical
                FROM (
                    SELECT
                        c.chunk_hash
                    FROM
                        chunks c
                    WHERE
                        TRUE",
        );

        if plan.required_terms.len() == 1 {
            for predicate in &plan.required_terms {
                push_content_condition(qb, predicate, case_mode, false);
            }
        } else if !plan.required_terms.is_empty() {
            qb.push(" AND (");
            for (idx, predicate) in plan.required_terms.iter().enumerate() {
                if idx > 0 {
                    qb.push(" OR ");
                }
                qb.push("(");
                push_content_predicate(qb, predicate, case_mode, "c.text_content");
                qb.push(")");
            }
            qb.push(")");
        }

        for predicate in &plan.excluded_terms {
            push_content_condition(qb, predicate, case_mode, true);
        }

        qb.push(
            "
                    LIMIT ",
        );
        qb.push_bind(plan_row_limit);
        qb.push(
            "
                ) chunk_hits
                JOIN content_blob_chunks cbc ON cbc.chunk_hash = chunk_hits.chunk_hash
                JOIN files ON files.content_hash = cbc.content_hash",
        );

        let needs_live_branch_filter_for_plan =
            plan.branches.is_empty() && !plan.include_historical;
        if needs_live_branch_filter_for_plan {
            qb.push(
                " LEFT JOIN live_repos lr ON lr.repository = files.repository
                      LEFT JOIN live_commits lc
                        ON lc.repository = files.repository
                       AND lc.commit_sha = files.commit_sha",
            );
        }

        let needs_language = !plan.langs.is_empty() || !plan.excluded_langs.is_empty();
        if needs_language {
            qb.push(" JOIN content_blobs cb ON cb.hash = files.content_hash");
        }

        qb.push(" WHERE TRUE");

        if !plan.repos.is_empty() {
            qb.push(" AND files.repository = ANY(");
            qb.push_bind(&plan.repos);
            qb.push(")");
        }

        if !plan.excluded_repos.is_empty() {
            qb.push(" AND NOT (files.repository = ANY(");
            qb.push_bind(&plan.excluded_repos);
            qb.push("))");
        }

        if !plan.file_globs.is_empty() {
            for pattern in &plan.file_globs {
                qb.push(" AND files.file_path ILIKE ");
                qb.push_bind(pattern);
                qb.push(" ESCAPE '\\'");
            }
        }

        if !plan.excluded_file_globs.is_empty() {
            for pattern in &plan.excluded_file_globs {
                qb.push(" AND files.file_path NOT ILIKE ");
                qb.push_bind(pattern);
                qb.push(" ESCAPE '\\'");
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

        if plan.required_terms.len() > 1 {
            qb.push(" AND files.content_hash IN (");
            for (idx, predicate) in plan.required_terms.iter().enumerate() {
                if idx > 0 {
                    qb.push(" INTERSECT ");
                }
                qb.push(
                    "SELECT DISTINCT cbc_req.content_hash \
                             FROM content_blob_chunks cbc_req \
                             JOIN chunks c_req ON c_req.chunk_hash = cbc_req.chunk_hash \
                             WHERE ",
                );
                push_content_predicate(qb, predicate, case_mode, "c_req.text_content");
            }
            qb.push(")");
        }

        if !plan.branches.is_empty() {
            qb.push(" AND (files.commit_sha = ANY(");
            qb.push_bind(&plan.branches);
            qb.push(") OR EXISTS (SELECT 1 FROM branches b WHERE b.repository = files.repository AND b.commit_sha = files.commit_sha AND b.branch = ANY(");
            qb.push_bind(&plan.branches);
            qb.push(")))");
        }

        if !plan.excluded_branches.is_empty() {
            qb.push(" AND NOT (files.commit_sha = ANY(");
            qb.push_bind(&plan.excluded_branches);
            qb.push(") OR EXISTS (SELECT 1 FROM branches b WHERE b.repository = files.repository AND b.commit_sha = files.commit_sha AND b.branch = ANY(");
            qb.push_bind(&plan.excluded_branches);
            qb.push(")))");
        }
        if needs_live_branch_filter_for_plan {
            qb.push(" AND (lr.repository IS NULL OR lc.commit_sha IS NOT NULL)");
        }
        qb.push(
            "
            )",
        );
    }

    qb.push(
        "),
            limited_plan AS (
                SELECT
                    pr.file_id,
                    pr.content_hash,
                    pr.line_count,
                    pr.chunk_index,
                    pr.highlight_pattern,
                    pr.highlight_case_sensitive,
                    pr.include_historical
                FROM
                    plan_results pr
                ORDER BY
                    pr.file_id,
                    pr.chunk_index
                LIMIT ",
    );
    qb.push_bind(fetch_limit);
    qb.push(
        "
            ),
            scored_files AS (
                SELECT
                    file_id,
                    content_hash,
                    include_historical,
                    SUM(
                        CASE
                            WHEN highlight_case_sensitive THEN 2
                            ELSE 1
                        END
                    ) AS score,
                    MIN(chunk_index) AS min_chunk_index
                FROM limited_plan
                GROUP BY file_id, content_hash, include_historical
            ),",
    );

    if symbol_terms.is_empty() {
        qb.push(
            "
            top_files AS (
                SELECT
                    sf.file_id,
                    sf.content_hash,
                    sf.include_historical,
                    sf.score::FLOAT8 AS total_score
                FROM scored_files sf
                ORDER BY sf.score DESC, sf.min_chunk_index ASC
                LIMIT ",
        );
        qb.push_bind(file_limit);
    } else {
        qb.push(
            "
            symbol_term_matches AS MATERIALIZED (
                SELECT
                    term,
                    us.name_lc
                FROM UNNEST(",
        );
        qb.push_bind(symbol_terms);
        qb.push(
            ") AS term
                JOIN unique_symbols us
                  ON us.name_lc LIKE '%' || term || '%'
            ),
            symbol_scores AS (
                SELECT
                    sf.file_id,
                    sf.content_hash,
                    MAX(
                        CASE
                            -- Strongly favor exact symbol matches, then namespace-prefixed, then loose substring
                            WHEN stm.name_lc = stm.term THEN 50.0
                            WHEN stm.name_lc LIKE stm.term || '::%' THEN 25.0
                            ELSE 1.0 / (1 + ABS(LENGTH(stm.name_lc) - LENGTH(stm.term)))
                        END
                    ) AS score
                FROM symbol_term_matches stm
                JOIN symbols s ON s.name_lc = stm.name_lc
                JOIN scored_files sf ON sf.content_hash = s.content_hash
                GROUP BY sf.file_id, sf.content_hash
            ),
            top_files AS (
                SELECT
                    sf.file_id,
                    sf.content_hash,
                    sf.include_historical,
                    (sf.score::FLOAT8 + COALESCE(ss.score, 0)::FLOAT8) AS total_score
                FROM scored_files sf
                LEFT JOIN symbol_scores ss
                  ON ss.file_id = sf.file_id
                 AND ss.content_hash = sf.content_hash
                LIMIT ",
        );
        qb.push_bind(file_limit);
    }

    qb.push(
        "
            ),
            ranked_top AS (
                SELECT
                    f.id AS file_id,
                    f.repository,
                    f.commit_sha,
                    f.file_path,
                    lp.content_hash,
                    MIN(lp.chunk_index) AS chunk_index,
                    tf.total_score,
                    tf.include_historical
                FROM limited_plan lp
                JOIN top_files tf
                  ON lp.file_id = tf.file_id
                 AND lp.content_hash = tf.content_hash
                 AND lp.include_historical = tf.include_historical
                JOIN files f
                  ON f.id = lp.file_id
                GROUP BY
                    f.id,
                    f.repository,
                    f.commit_sha,
                    f.file_path,
                    lp.content_hash,
                    tf.total_score,
                    tf.include_historical
            ),
            ranked_keys AS (
                SELECT DISTINCT repository, commit_sha
                FROM ranked_top
            ),
            branch_match AS (
                SELECT
                    bs.repository,
                    bs.commit_sha,
                    array_agg(DISTINCT bs.branch) AS branches,
                    MAX(bs.indexed_at) AS snapshot_indexed_at
                FROM branch_snapshots bs
                JOIN ranked_keys rk
                  ON rk.repository = bs.repository
                 AND rk.commit_sha = bs.commit_sha
                GROUP BY bs.repository, bs.commit_sha
            ),
            branch_fallback AS (
                SELECT
                    b.repository,
                    b.commit_sha,
                    array_agg(DISTINCT b.branch) AS fallback_branches
                FROM branches b
                JOIN ranked_keys rk
                  ON rk.repository = b.repository
                 AND rk.commit_sha = b.commit_sha
                GROUP BY b.repository, b.commit_sha
            ),
            live_branch_match AS (
                SELECT
                    rk.repository,
                    rk.commit_sha,
                    array_agg(lb.branch) AS live_branches
                FROM ranked_keys rk
                JOIN repo_live_branches lb
                  ON lb.repository = rk.repository
                JOIN branch_snapshots bs
                  ON bs.repository = lb.repository
                 AND bs.branch = lb.branch
                 AND bs.commit_sha = rk.commit_sha
                GROUP BY rk.repository, rk.commit_sha
            ),
            live_branch_fallback AS (
                SELECT
                    rk.repository,
                    rk.commit_sha,
                    array_agg(lb.branch) AS live_branches
                FROM ranked_keys rk
                JOIN repo_live_branches lb
                  ON lb.repository = rk.repository
                JOIN branches b
                  ON b.repository = lb.repository
                 AND b.branch = lb.branch
                 AND b.commit_sha = rk.commit_sha
                GROUP BY rk.repository, rk.commit_sha
            ),
            live_repo AS (
                SELECT
                    rk.repository,
                    array_agg(lb.branch) AS repo_live_branches
                FROM ranked_keys rk
                LEFT JOIN repo_live_branches lb
                  ON lb.repository = rk.repository
                GROUP BY rk.repository
            ),
            filtered_ranked AS (
                SELECT
                    rt.file_id,
                    rt.repository,
                    rt.commit_sha,
                    rt.file_path,
                    rt.content_hash,
                    rt.chunk_index,
                    rt.total_score,
                    rt.include_historical,
                    COALESCE(bm.branches, bf.fallback_branches, ARRAY[]::TEXT[]) AS branches,
                    COALESCE(
                        lbm.live_branches,
                        lbf.live_branches,
                        ARRAY[]::TEXT[]
                    ) AS live_branches,
                    bm.snapshot_indexed_at AS snapshot_indexed_at,
                    CASE
                        WHEN lr.repo_live_branches IS NULL THEN FALSE
                        WHEN COALESCE(
                                array_length(
                                    COALESCE(
                                        lbm.live_branches,
                                        lbf.live_branches,
                                        ARRAY[]::TEXT[]
                                    ),
                                    1
                                ),
                                0
                            ) = 0 THEN TRUE
                        ELSE FALSE
                        END AS is_historical
                FROM ranked_top rt
                LEFT JOIN branch_match bm
                  ON bm.repository = rt.repository
                 AND bm.commit_sha = rt.commit_sha
                LEFT JOIN branch_fallback bf
                  ON bf.repository = rt.repository
                 AND bf.commit_sha = rt.commit_sha
                LEFT JOIN live_branch_match lbm
                  ON lbm.repository = rt.repository
                 AND lbm.commit_sha = rt.commit_sha
                LEFT JOIN live_branch_fallback lbf
                  ON lbf.repository = rt.repository
                 AND lbf.commit_sha = rt.commit_sha
                LEFT JOIN live_repo lr
                  ON lr.repository = rt.repository
                WHERE
                    rt.include_historical
                    OR lr.repo_live_branches IS NULL
                        OR COALESCE(
                            array_length(
                                COALESCE(
                                    lbm.live_branches,
                                    lbf.live_branches,
                                    ARRAY[]::TEXT[]
                                ),
                                1
                            ),
                            0
                        ) > 0
                        OR COALESCE(
                            array_length(
                                COALESCE(
                                    bm.branches,
                                    bf.fallback_branches,
                                    ARRAY[]::TEXT[]
                                ),
                                1
                            ),
                            0
                        ) > 0
            )",
    );
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

    async fn get_branches_for_repository(
        &self,
        repository: &str,
    ) -> Result<Vec<RepoBranchInfo>, DbError> {
        let rows = sqlx::query!(
            r#"
            SELECT
                b.branch,
                b.commit_sha,
                lb.branch IS NOT NULL AS is_live,
                COALESCE(snapshot.latest_indexed_at, b.indexed_at) AS indexed_at
            FROM branches b
            LEFT JOIN repo_live_branches lb
              ON lb.repository = b.repository
             AND lb.branch = b.branch
            LEFT JOIN LATERAL (
                SELECT MAX(indexed_at) AS latest_indexed_at
                FROM branch_snapshots bs
                WHERE bs.repository = b.repository AND bs.branch = b.branch
            ) snapshot ON TRUE
            WHERE b.repository = $1
            ORDER BY b.branch
            "#,
            repository
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if rows.is_empty() {
            let commits: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 ORDER BY commit_sha DESC",
            )
            .bind(repository)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

            let fallback = commits
                .into_iter()
                .map(|commit| RepoBranchInfo {
                    name: commit.clone(),
                    commit_sha: commit,
                    indexed_at: None,
                    is_live: false,
                })
                .collect();
            return Ok(fallback);
        }

        let branches = rows
            .into_iter()
            .map(|row| RepoBranchInfo {
                name: row.branch,
                commit_sha: row.commit_sha,
                indexed_at: row.indexed_at.map(|dt| dt.to_rfc3339()),
                is_live: row.is_live.unwrap_or(false),
            })
            .collect();

        Ok(branches)
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

    async fn search_repo_paths(
        &self,
        repository: &str,
        commit_sha: &str,
        query: &str,
        limit: i64,
    ) -> Result<Vec<TreeEntry>, DbError> {
        if commit_sha.is_empty() {
            return Err(DbError::Internal("missing commit parameter".to_string()));
        }

        let trimmed = query.trim();
        if trimmed.is_empty() || limit <= 0 {
            return Ok(Vec::new());
        }

        let mut escaped = String::with_capacity(trimmed.len());
        for ch in trimmed.chars() {
            match ch {
                '%' | '_' | '\\' => {
                    escaped.push('\\');
                    escaped.push(ch);
                }
                _ => escaped.push(ch),
            }
        }
        let pattern = format!("%{escaped}%");
        let fetch_limit = (limit.saturating_mul(5)).clamp(1, 200);

        let rows: Vec<String> = sqlx::query_scalar(
            "SELECT file_path
             FROM files
             WHERE repository = $1
             AND commit_sha = $2
             AND file_path ILIKE $3 ESCAPE '\\'
             ORDER BY file_path
             LIMIT $4",
        )
        .bind(repository)
        .bind(commit_sha)
        .bind(&pattern)
        .bind(fetch_limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let query_lower = trimmed.to_ascii_lowercase();
        let mut dir_set: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut file_paths: Vec<String> = Vec::new();
        let mut seen_files: std::collections::HashSet<String> = std::collections::HashSet::new();

        for path in rows {
            let lower = path.to_ascii_lowercase();
            if lower.contains(&query_lower) && seen_files.insert(path.clone()) {
                file_paths.push(path.clone());
            }

            let mut segments: Vec<&str> = path.split('/').collect();
            if segments.len() > 1 {
                segments.pop();
                while !segments.is_empty() {
                    let dir = segments.join("/");
                    if dir.to_ascii_lowercase().contains(&query_lower) {
                        dir_set.insert(dir.clone());
                    }
                    segments.pop();
                }
            }
        }

        let mut directories: Vec<String> = dir_set.into_iter().collect();
        directories.sort();

        let mut entries = Vec::new();
        for dir in directories {
            let name = dir.rsplit('/').next().unwrap_or(&dir).to_string();
            entries.push(TreeEntry {
                name,
                path: dir,
                kind: "dir".to_string(),
            });
            if entries.len() as i64 >= limit {
                return Ok(entries);
            }
        }

        for path in file_paths {
            let name = path.rsplit('/').next().unwrap_or(&path).to_string();
            entries.push(TreeEntry {
                name,
                path,
                kind: "file".to_string(),
            });
            if entries.len() as i64 >= limit {
                break;
            }
        }

        Ok(entries)
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
        let snippets = self.get_file_snippets(vec![request]).await?;
        snippets
            .into_iter()
            .next()
            .ok_or_else(|| DbError::Internal("missing snippet response".to_string()))
    }

    async fn get_file_snippets(
        &self,
        requests: Vec<SnippetRequest>,
    ) -> Result<Vec<SnippetResponse>, DbError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let total = requests.len();
        let mut repositories = Vec::with_capacity(total);
        let mut commits = Vec::with_capacity(total);
        let mut paths = Vec::with_capacity(total);
        let mut lines = Vec::with_capacity(total);
        let mut contexts = Vec::with_capacity(total);

        for request in requests {
            if request.line == 0 {
                return Err(DbError::Internal("line numbers are 1-based".to_string()));
            }

            repositories.push(request.repository);
            commits.push(request.commit_sha);
            paths.push(request.file_path);
            lines.push(i32::try_from(request.line).unwrap_or(i32::MAX));
            contexts.push(request.context.unwrap_or(3).min(3) as i32);
        }

        let rows: Vec<SnippetRow> = sqlx::query_as(
            r#"
WITH req AS (
    SELECT
        (ordinality - 1)::int AS idx,
        repo,
        commit_sha,
        file_path,
        line,
        context
    FROM
        unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::int[])
        WITH ORDINALITY AS t(repo, commit_sha, file_path, line, context, ordinality)
), data AS (
    SELECT
        req.idx,
        req.line,
        req.context,
        cb.line_count,
        string_agg(chunks.text_content, '' ORDER BY cbc.chunk_index) AS text_content
    FROM req
    JOIN files f
      ON f.repository = req.repo
     AND f.commit_sha = req.commit_sha
     AND f.file_path = req.file_path
    JOIN content_blobs cb
      ON cb.hash = f.content_hash
    JOIN content_blob_chunks cbc
      ON cbc.content_hash = cb.hash
    JOIN chunks
      ON chunks.chunk_hash = cbc.chunk_hash
    GROUP BY req.idx, req.line, req.context, cb.line_count
)
SELECT
    idx,
    line,
    context,
    line_count,
    GREATEST(line - context, 1) AS start_line,
    LEAST(line + context, line_count) AS end_line,
    array_to_string(
        (string_to_array(text_content, E'\n'))[
            GREATEST(line - context, 1):
            LEAST(line + context, line_count)
        ],
        E'\n'
    ) AS snippet
FROM data
ORDER BY idx
            "#,
        )
        .bind(&repositories)
        .bind(&commits)
        .bind(&paths)
        .bind(&lines)
        .bind(&contexts)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| DbError::Database(e.to_string()))?;

        let mut responses: Vec<Option<SnippetResponse>> = vec![None; total];

        for row in rows {
            let idx = usize::try_from(row.idx)
                .map_err(|_| DbError::Internal("invalid snippet index".to_string()))?;
            if idx >= responses.len() {
                return Err(DbError::Internal("snippet index out of bounds".to_string()));
            }

            let snippet_text = row.snippet.unwrap_or_default();
            let lines_vec: Vec<String> = if snippet_text.is_empty() {
                Vec::new()
            } else {
                snippet_text.split('\n').map(|s| s.to_string()).collect()
            };

            let start_line = row.start_line.max(1) as u32;
            let highlight_line = row.line.max(1) as u32;
            let total_lines = row.line_count.max(0) as u32;
            let end_line = row.end_line.max(row.start_line);
            let truncated = start_line > 1 || end_line < row.line_count;

            responses[idx] = Some(SnippetResponse {
                start_line,
                highlight_line,
                total_lines,
                lines: lines_vec,
                truncated,
            });
        }

        responses
            .into_iter()
            .map(|snippet| {
                snippet.ok_or_else(|| DbError::Internal("missing snippet response".to_string()))
            })
            .collect()
    }

    async fn get_symbol_references(
        &self,
        request: SymbolReferenceRequest,
    ) -> Result<SymbolReferenceResponse, DbError> {
        let (namespace_opt, name) = split_fully_qualified(&request.fully_qualified);
        let mut namespace_filter = namespace_opt
            .filter(|ns| !ns.is_empty())
            .map(|ns| ns.to_string());
        let mut symbol_ids: Vec<i32> = Vec::new();

        if let (Some(path), Some(line)) = (&request.file_path, request.line) {
            let line_i32 = i32::try_from(line).unwrap_or(i32::MAX);
            let mut qb = QueryBuilder::new(
                "SELECT s.id, NULLIF(sn.namespace, '') AS namespace \
                 FROM symbol_references sr \
                 JOIN symbols s ON s.id = sr.symbol_id \
                 JOIN symbol_namespaces sn ON sn.id = sr.namespace_id \
                 JOIN files f ON f.content_hash = s.content_hash \
                 WHERE f.repository = ",
            );
            qb.push_bind(&request.repository)
                .push(" AND f.commit_sha = ")
                .push_bind(&request.commit_sha)
                .push(" AND f.file_path = ")
                .push_bind(path)
                .push(" AND sr.kind = 'definition' AND sr.line_number = ")
                .push_bind(line_i32);

            if let Some(column) = request.column {
                let column_i32 = i32::try_from(column).unwrap_or(i32::MAX);
                qb.push(" AND sr.column_number = ").push_bind(column_i32);
            }

            qb.push(" ORDER BY sr.line_number, sr.column_number LIMIT 8");

            let def_rows: Vec<(i32, Option<String>)> = qb
                .build_query_as()
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

            for (symbol_id, ns) in def_rows {
                symbol_ids.push(symbol_id);
                if namespace_filter.is_none() {
                    if let Some(ns_val) = ns.filter(|ns| !ns.is_empty()) {
                        namespace_filter = Some(ns_val);
                    }
                }
            }
        }

        let mut qb = QueryBuilder::new(
            "SELECT f.repository, f.commit_sha, f.file_path, NULLIF(sn.namespace, '') AS namespace, s.name AS name, sr.kind, \
                    sr.line_number AS line, sr.column_number AS column \
             FROM symbol_references sr \
             JOIN symbols s ON s.id = sr.symbol_id \
             JOIN symbol_namespaces sn ON sn.id = sr.namespace_id \
             JOIN files f ON f.content_hash = s.content_hash \
             WHERE f.repository = ",
        );
        qb.push_bind(&request.repository)
            .push(" AND f.commit_sha = ")
            .push_bind(&request.commit_sha);

        if !symbol_ids.is_empty() {
            qb.push(" AND sr.symbol_id = ANY(")
                .push_bind(&symbol_ids)
                .push(")");
        } else {
            qb.push(" AND s.name = ").push_bind(&name);
            if let Some(ns) = namespace_filter {
                qb.push(" AND COALESCE(sn.namespace, '') = ").push_bind(ns);
            }
        }

        qb.push(" ORDER BY f.file_path, sr.line_number, sr.column_number");

        let rows: Vec<DbFileReference> = qb
            .build_query_as()
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
        let needle = request.name.clone();
        let namespace_hint = request
            .namespace
            .clone()
            .or_else(|| request.namespace_prefix.clone());

        let matching_hashes = if let Some(q) = &request.q {
            let hashes: Vec<String> = sqlx::query_scalar(
                "SELECT DISTINCT cbc.content_hash \
                 FROM chunks c \
                 JOIN content_blob_chunks cbc ON c.chunk_hash = cbc.chunk_hash \
                 WHERE c.text_content LIKE '%' || $1 || '%'",
            )
            .bind(q)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

            if hashes.is_empty() {
                return Ok(SearchResponse {
                    symbols: Vec::new(),
                });
            }

            Some(hashes)
        } else {
            None
        };

        let mut qb = QueryBuilder::new(
            "WITH ranked AS ( \
                 SELECT DISTINCT ON (s.id) \
                     s.id, \
                     s.name AS symbol, \
                     NULLIF(sn.namespace, '') AS namespace, \
                     COALESCE(sr.kind, 'definition') AS kind, \
                     CASE \
                         WHEN sn.namespace IS NULL OR sn.namespace = '' THEN s.name \
                         ELSE sn.namespace || '::' || s.name \
                     END AS fully_qualified, \
                     cb.language, \
                     f.repository, \
                     f.commit_sha, \
                    f.file_path, \
                    sr.line_number AS line_number, \
                    sr.column_number AS column_number, \
                    symbol_weight( \
                        s.name, \
                        CASE \
                            WHEN sn.namespace IS NULL OR sn.namespace = '' THEN s.name \
                            ELSE sn.namespace || '::' || s.name \
                        END, \
                        NULLIF(sn.namespace, ''), \
                        COALESCE(sr.kind, 'definition'), \
                        ",
        );
        qb.push_bind(needle.as_deref());
        qb.push(
            ", \
                        ",
        );
        qb.push_bind(namespace_hint.as_deref());
        qb.push(
            ", \
                        f.file_path, \
                        ",
        );

        let path_hint = request.path_hint.clone().or(request.path.clone());
        qb.push_bind(path_hint.as_deref());

        qb.push(
            ") AS score \
                 FROM symbols s \
                 JOIN symbol_references sr ON sr.symbol_id = s.id \
                 JOIN symbol_namespaces sn ON sn.id = sr.namespace_id \
                 JOIN files f ON f.content_hash = s.content_hash \
                 LEFT JOIN content_blobs cb ON cb.hash = s.content_hash \
                 WHERE 1=1",
        );

        if let Some(hashes) = matching_hashes {
            qb.push(" AND s.content_hash = ANY(")
                .push_bind(hashes)
                .push(")");
        }

        if let Some(name) = &request.name {
            qb.push(" AND s.name = ").push_bind(name);
        }

        if let Some(regex) = &request.name_regex {
            qb.push(" AND s.name ~ ").push_bind(regex);
        }

        if let Some(namespace) = &request.namespace {
            qb.push(" AND sn.namespace = ").push_bind(namespace);
        }

        if let Some(prefix) = &request.namespace_prefix {
            qb.push(" AND sn.namespace LIKE ")
                .push_bind(format!("{}%", prefix));
        }

        if let Some(kinds) = &request.kind {
            if !kinds.is_empty() {
                qb.push(" AND COALESCE(sr.kind, 'definition') = ANY(")
                    .push_bind(kinds)
                    .push(")");
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

        if !request.include_paths.is_empty() {
            qb.push(
                " AND EXISTS (
                    SELECT 1
                    FROM unnest(",
            )
            .push_bind(&request.include_paths)
            .push(
                ") AS include_path(value)
                    WHERE
                        f.file_path = include_path.value
                        OR (
                            RIGHT(include_path.value, 1) = '/'
                            AND f.file_path LIKE include_path.value || '%'
                        )
                )",
            );
        }

        if !request.excluded_paths.is_empty() {
            qb.push(
                " AND NOT EXISTS (
                    SELECT 1
                    FROM unnest(",
            )
            .push_bind(&request.excluded_paths)
            .push(
                ") AS excluded_path(value)
                    WHERE
                        f.file_path = excluded_path.value
                        OR (
                            RIGHT(excluded_path.value, 1) = '/'
                            AND f.file_path LIKE excluded_path.value || '%'
                        )
                )",
            );
        }

        qb.push(
            " ORDER BY \
                 s.id, \
                 score DESC, \
                 (sr.kind = 'definition') DESC, \
                 sr.line_number, \
                 sr.column_number \
             ) ",
        );

        let include_refs = request.include_references.unwrap_or(false);
        if include_refs {
            qb.push(
                "SELECT ranked.id, ranked.symbol, ranked.namespace, ranked.kind, ranked.fully_qualified, ranked.language, \
                        ranked.repository, ranked.commit_sha, ranked.file_path, ranked.line_number, ranked.column_number, ranked.score, \
                        refs.references \
                 FROM ranked \
                 LEFT JOIN LATERAL ( \
                     SELECT jsonb_agg( \
                         jsonb_build_object( \
                             'namespace', NULLIF(sn_all.namespace, ''), \
                             'name', ranked.symbol, \
                             'kind', sr_all.kind, \
                             'line', sr_all.line_number, \
                             'column', sr_all.column_number, \
                             'repository', ranked.repository, \
                             'commit_sha', ranked.commit_sha, \
                             'file_path', ranked.file_path \
                         ) ORDER BY sr_all.line_number, sr_all.column_number \
                     ) AS references \
                     FROM symbol_references sr_all \
                     JOIN symbol_namespaces sn_all ON sn_all.id = sr_all.namespace_id \
                     WHERE sr_all.symbol_id = ranked.id \
                 ) refs ON TRUE \
                 ORDER BY ranked.score DESC, ranked.symbol ASC LIMIT ",
            );
        } else {
            qb.push(
                "SELECT ranked.id, ranked.symbol, ranked.namespace, ranked.kind, ranked.fully_qualified, ranked.language, \
                        ranked.repository, ranked.commit_sha, ranked.file_path, ranked.line_number, ranked.column_number, ranked.score, \
                        NULL::jsonb AS references \
                 FROM ranked \
                 ORDER BY ranked.score DESC, ranked.symbol ASC LIMIT ",
            );
        }

        let limit = request.limit.unwrap_or(100).clamp(1, 1000);
        qb.push_bind(limit);

        let rows: Vec<SymbolRow> = qb
            .build_query_as()
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let references = if include_refs {
                row.references.as_ref().map(|refs_json| {
                    refs_json
                        .0
                        .iter()
                        .map(|r| ReferenceResult {
                            name: r.name.clone(),
                            namespace: r.namespace.clone(),
                            kind: r.kind.clone(),
                            fully_qualified: r
                                .namespace
                                .as_ref()
                                .map(|ns| format!("{}::{}", ns, r.name))
                                .unwrap_or_else(|| r.name.clone()),
                            repository: r.repository.clone(),
                            commit_sha: r.commit_sha.clone(),
                            file_path: r.file_path.clone(),
                            line: r.line.unwrap_or_default().max(0) as usize,
                            column: r.column.unwrap_or_default().max(0) as usize,
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

            let kind = row.kind.clone().unwrap_or_else(|| "definition".to_string());

            tracing::debug!(
                target: "pointer::search_symbols",
                symbol = %row.fully_qualified,
                score = row.score,
                repository = %row.repository,
                file_path = %row.file_path,
                kind = %kind,
                "symbol ranking debug"
            );

            results.push(SymbolResult {
                symbol: row.symbol,
                namespace: row.namespace,
                kind: Some(kind),
                fully_qualified: row.fully_qualified,
                repository: row.repository,
                commit_sha: row.commit_sha,
                file_path: row.file_path,
                language: row.language,
                line,
                column,
                references,
                score: row.score,
            });
        }

        Ok(SearchResponse { symbols: results })
    }

    async fn text_search(&self, request: &TextSearchRequest) -> Result<SearchResultsPage, DbError> {
        if request.plans.is_empty() {
            return Ok(SearchResultsPage::empty(
                request.original_query.clone(),
                request.page,
                request.page_size,
            ));
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
        let mut fetch_limit = fetch_limit_u64.min(i64::MAX as u64) as i64;
        fetch_limit = fetch_limit.min(5000);
        let file_limit = fetch_limit.min(25000);
        let plan_row_limit: i64 = 5000;

        let needs_live_branch_filter = request
            .plans
            .iter()
            .any(|plan| plan.branches.is_empty() && !plan.include_historical);

        let symbol_terms: Vec<String> = collect_symbol_terms(request)
            .into_iter()
            .map(|t| t.to_lowercase())
            .collect();

        let explain_requested = std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok();

        let mut phase1_qb = QueryBuilder::new("");
        push_search_ctes(
            &mut phase1_qb,
            request,
            plan_row_limit,
            fetch_limit,
            file_limit,
            needs_live_branch_filter,
            &symbol_terms,
        );
        phase1_qb.push(
            "
            SELECT
                fr.repository,
                fr.commit_sha,
                fr.file_path,
                fr.content_hash,
                fr.chunk_index,
                fr.total_score,
                fr.include_historical,
                fr.branches,
                fr.live_branches,
                fr.is_historical,
                fr.snapshot_indexed_at
            FROM filtered_ranked fr
            ORDER BY
                fr.total_score DESC,
                fr.repository,
                fr.commit_sha,
                fr.file_path,
                fr.chunk_index
            LIMIT ",
        );
        phase1_qb.push_bind(fetch_limit);

        let mut phase1_query = phase1_qb.build_query_as::<RankedFileRow>();

        if explain_requested {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", phase1_query.sql());
            if let Ok(Some(args)) = phase1_query.take_arguments() {
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, _>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::text_search_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(target: "pointer::text_search_sql", "failed to run EXPLAIN: {}", err);
                    }
                }
                phase1_query = sqlx::query_as_with::<_, RankedFileRow, _>(phase1_query.sql(), args);
            }
        }

        let ranked_rows = phase1_query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        let row_limit_hit = (ranked_rows.len() as i64) >= fetch_limit;

        if ranked_rows.is_empty() {
            return Ok(SearchResultsPage::empty(
                request.original_query.clone(),
                request.page,
                request.page_size,
            ));
        }

        let total = ranked_rows.len();
        let page_index = request.page.saturating_sub(1) as usize;
        let page_size = request.page_size as usize;
        let start = page_index.saturating_mul(page_size);
        let mut has_more = total > start + page_size;
        if !has_more && total > 0 && row_limit_hit {
            has_more = true;
        }

        let stats = build_search_stats(&ranked_rows);

        let results = if start >= total {
            Vec::new()
        } else {
            let offset = i64::try_from(start).unwrap_or(i64::MAX);
            let limit = i64::try_from(page_size).unwrap_or(i64::MAX);

            let mut phase2_qb = QueryBuilder::new("");
            push_search_ctes(
                &mut phase2_qb,
                request,
                plan_row_limit,
                fetch_limit,
                file_limit,
                needs_live_branch_filter,
                &symbol_terms,
            );
            phase2_qb.push(
                ",
            paged_files AS (
                SELECT
                    fr.file_id,
                    fr.repository,
                    fr.commit_sha,
                    fr.file_path,
                    fr.content_hash,
                    fr.chunk_index,
                    fr.total_score,
                    fr.include_historical,
                    fr.branches,
                    fr.live_branches,
                    fr.is_historical,
                    fr.snapshot_indexed_at
                FROM filtered_ranked fr
                ORDER BY
                    fr.total_score DESC,
                    fr.repository,
                    fr.commit_sha,
                    fr.file_path,
                    fr.chunk_index
                LIMIT ",
            );
            phase2_qb.push_bind(limit);
            phase2_qb.push(" OFFSET ");
            phase2_qb.push_bind(offset);
            phase2_qb.push(
                "
            )
            SELECT
                pf.repository,
                pf.commit_sha,
                pf.file_path,
                pf.content_hash,
                sl.start_line,
                cbc.chunk_line_count AS line_count,
                ctx.context_snippet AS content_text,
                ctx.match_line_number,
                pf.branches,
                pf.live_branches,
                pf.is_historical,
                pf.snapshot_indexed_at
            FROM paged_files pf
            JOIN content_blob_chunks cbc
              ON cbc.content_hash = pf.content_hash
             AND cbc.chunk_index = pf.chunk_index
            JOIN chunks c
              ON c.chunk_hash = cbc.chunk_hash
            JOIN limited_plan lp
              ON lp.file_id = pf.file_id
             AND lp.content_hash = pf.content_hash
             AND lp.chunk_index = pf.chunk_index
             AND lp.include_historical = pf.include_historical
            CROSS JOIN LATERAL extract_context_with_highlight(
                c.text_content,
                lp.highlight_pattern,
                3,
                lp.highlight_case_sensitive
            ) ctx
            LEFT JOIN LATERAL (
                SELECT
                    1 + COALESCE(SUM(cbc.chunk_line_count), 0) AS start_line
                FROM content_blob_chunks cbc
                WHERE cbc.content_hash = pf.content_hash
                  AND cbc.chunk_index < pf.chunk_index
            ) sl ON TRUE
            ORDER BY
                pf.total_score DESC,
                pf.repository,
                pf.commit_sha,
                pf.file_path,
                pf.chunk_index,
                ctx.match_line_number",
            );

            let mut phase2_query = phase2_qb.build_query_as::<SearchResultRow>();

            if explain_requested {
                let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", phase2_query.sql());
                if let Ok(Some(args)) = phase2_query.take_arguments() {
                    let explain_args = args.clone();
                    match sqlx::query_scalar_with::<Postgres, String, _>(&sql, explain_args)
                        .fetch_all(&self.pool)
                        .await
                    {
                        Ok(rows) => {
                            for line in rows {
                                tracing::info!(target: "pointer::text_search_sql", "{}", line);
                            }
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "pointer::text_search_sql",
                                "failed to run EXPLAIN: {}",
                                err
                            );
                        }
                    }
                    phase2_query =
                        sqlx::query_as_with::<_, SearchResultRow, _>(phase2_query.sql(), args);
                }
            }

            let rows = phase2_query
                .fetch_all(&self.pool)
                .await
                .map_err(|e| DbError::Database(e.to_string()))?;

            let mut aggregates: Vec<FileAggregate> = Vec::new();
            let mut index_map: HashMap<FileGroupKey, usize> = HashMap::new();
            for row in rows.into_iter() {
                let key = FileGroupKey {
                    repository: row.repository.clone(),
                    commit_sha: row.commit_sha.clone(),
                    file_path: row.file_path.clone(),
                    content_hash: row.content_hash.clone(),
                };
                if let Some(&idx) = index_map.get(&key) {
                    aggregates[idx].entries.push(row);
                } else {
                    index_map.insert(key, aggregates.len());
                    aggregates.push(FileAggregate { entries: vec![row] });
                }
            }

            aggregates
                .into_iter()
                .map(|mut agg| {
                    agg.entries.sort_by(|a, b| {
                        let (exact_a, marks_a, signal_a) = snippet_signal_score(&a.content_text);
                        let (exact_b, marks_b, signal_b) = snippet_signal_score(&b.content_text);
                        exact_b
                            .cmp(&exact_a)
                            .then_with(|| marks_b.cmp(&marks_a))
                            .then_with(|| signal_b.cmp(&signal_a))
                            .then_with(|| a.match_line_number.cmp(&b.match_line_number))
                            .then_with(|| a.start_line.cmp(&b.start_line))
                    });

                    let mut entries_iter = agg.entries.into_iter();
                    let best_row = entries_iter
                        .next()
                        .expect("aggregated results should contain at least one snippet");

                    let chunk_start_line: i32 = best_row.start_line.try_into().unwrap_or(i32::MAX);
                    let best_match_line =
                        chunk_start_line.saturating_add(best_row.match_line_number - 1);
                    let (best_start_line, best_end_line) =
                        snippet_bounds(&best_row.content_text, best_match_line);

                    let mut snippets = Vec::new();
                    snippets.push(SearchSnippet {
                        start_line: best_start_line,
                        end_line: best_end_line,
                        match_line: best_match_line,
                        content_text: best_row.content_text.clone(),
                    });

                    for row in entries_iter {
                        let chunk_start_line: i32 = row.start_line.try_into().unwrap_or(i32::MAX);
                        let snippet_match =
                            chunk_start_line.saturating_add(row.match_line_number - 1);
                        let (snippet_start, snippet_end) =
                            snippet_bounds(&row.content_text, snippet_match);
                        snippets.push(SearchSnippet {
                            start_line: snippet_start,
                            end_line: snippet_end,
                            match_line: snippet_match,
                            content_text: row.content_text,
                        });
                    }

                    let merged_snippets = merge_overlapping_snippets(snippets);
                    let primary_snippet = merged_snippets
                        .iter()
                        .find(|snippet| {
                            snippet.start_line <= best_match_line
                                && snippet.end_line >= best_match_line
                        })
                        .cloned()
                        .or_else(|| merged_snippets.first().cloned())
                        .unwrap_or_else(|| SearchSnippet {
                            start_line: best_start_line,
                            end_line: best_end_line,
                            match_line: best_match_line,
                            content_text: best_row.content_text.clone(),
                        });

                    SearchResult {
                        repository: best_row.repository,
                        commit_sha: best_row.commit_sha,
                        file_path: best_row.file_path,
                        start_line: primary_snippet.start_line,
                        end_line: primary_snippet.end_line,
                        match_line: primary_snippet.match_line,
                        content_text: primary_snippet.content_text.clone(),
                        snippets: merged_snippets,
                        branches: best_row.branches,
                        live_branches: best_row.live_branches,
                        is_historical: best_row.is_historical,
                        snapshot_indexed_at: best_row
                            .snapshot_indexed_at
                            .as_ref()
                            .map(|dt| dt.to_rfc3339()),
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
            stats,
        })
    }

    async fn autocomplete_repositories(
        &self,
        term: &str,
        limit: i64,
    ) -> Result<Vec<String>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);
        let mut query = sqlx::query_scalar(
            "SELECT DISTINCT repository \
             FROM files \
             WHERE repository ILIKE $1 ESCAPE '\\' \
             ORDER BY repository \
             LIMIT $2",
        )
        .bind(pattern)
        .bind(limit);

        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_scalar_with::<Postgres, String, PgArguments>(query.sql(), args);
            }
        }

        let rows: Vec<String> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows)
    }

    async fn autocomplete_paths(
        &self,
        repositories: &[String],
        term: &str,
        limit: i64,
    ) -> Result<Vec<String>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);

        let mut qb = QueryBuilder::new(
            "WITH dirs AS (\
                SELECT DISTINCT \
                    CASE \
                        WHEN position('/' in file_path) > 0 \
                        THEN regexp_replace(file_path, '/[^/]+$', '') || '/*' \
                        ELSE '/*' \
                    END AS dir \
                FROM files",
        );

        if !repositories.is_empty() {
            qb.push(" WHERE repository = ANY(");
            qb.push_bind(repositories);
            qb.push(")");
        }

        qb.push(
            ") \
            SELECT dir \
            FROM dirs \
            WHERE dir ILIKE ",
        );
        qb.push_bind(pattern);
        qb.push(" ESCAPE '\\' ORDER BY dir LIMIT ");
        qb.push_bind(limit);

        let mut query = qb.build_query_scalar::<String>();
        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_scalar_with::<Postgres, String, PgArguments>(query.sql(), args);
            }
        }

        let rows: Vec<String> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows)
    }

    async fn autocomplete_files(
        &self,
        repositories: &[String],
        term: &str,
        limit: i64,
    ) -> Result<Vec<String>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);

        let mut qb = QueryBuilder::new(
            "SELECT DISTINCT file_path \
             FROM files \
             WHERE TRUE",
        );
        if !repositories.is_empty() {
            qb.push(" AND repository = ANY(");
            qb.push_bind(repositories);
            qb.push(")");
        }
        qb.push(" AND file_path ILIKE ");
        qb.push_bind(pattern);
        qb.push(" ESCAPE '\\' ORDER BY file_path LIMIT ");
        qb.push_bind(limit);

        let mut query = qb.build_query_scalar::<String>();
        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_scalar_with::<Postgres, String, PgArguments>(query.sql(), args);
            }
        }

        let rows: Vec<String> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows)
    }

    async fn autocomplete_languages(
        &self,
        repositories: &[String],
        term: &str,
        limit: i64,
    ) -> Result<Vec<String>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);

        let mut qb = QueryBuilder::new(
            "SELECT DISTINCT cb.language \
             FROM content_blobs cb \
             JOIN files f ON f.content_hash = cb.hash \
             WHERE TRUE",
        );
        if !repositories.is_empty() {
            qb.push(" AND f.repository = ANY(");
            qb.push_bind(repositories);
            qb.push(")");
        }
        qb.push(" AND cb.language IS NOT NULL AND cb.language ILIKE ");
        qb.push_bind(pattern);
        qb.push(" ESCAPE '\\' ORDER BY cb.language LIMIT ");
        qb.push_bind(limit);

        let mut query = qb.build_query_scalar::<Option<String>>();
        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_scalar_with::<Postgres, Option<String>, PgArguments>(
                    query.sql(),
                    args,
                );
            }
        }

        let rows: Vec<Option<String>> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows.into_iter().flatten().collect())
    }

    async fn autocomplete_branches(
        &self,
        repositories: &[String],
        term: &str,
        limit: i64,
    ) -> Result<Vec<String>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);

        let mut qb = QueryBuilder::new(
            "SELECT DISTINCT branch \
             FROM branches \
             WHERE TRUE",
        );
        if !repositories.is_empty() {
            qb.push(" AND repository = ANY(");
            qb.push_bind(repositories);
            qb.push(")");
        }
        qb.push(" AND branch ILIKE ");
        qb.push_bind(pattern);
        qb.push(" ESCAPE '\\' ORDER BY branch LIMIT ");
        qb.push_bind(limit);

        let mut query = qb.build_query_scalar::<String>();
        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_scalar_with::<Postgres, String, PgArguments>(query.sql(), args);
            }
        }

        let rows: Vec<String> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows)
    }

    async fn autocomplete_symbols(
        &self,
        term: &str,
        limit: i64,
    ) -> Result<Vec<SymbolSuggestion>, DbError> {
        let escaped = escape_sql_like_literal(term);
        let pattern = format!("%{}%", escaped);
        let mut query = sqlx::query_as(
            "WITH matches AS (
                SELECT us.name_lc
                FROM unique_symbols us
                WHERE us.name_lc ILIKE $1 ESCAPE '\\'
                LIMIT $2
             )
             SELECT
                m.name_lc,
                MIN(f.repository) AS repository,
                MIN(f.file_path) AS file_path
             FROM matches m
             JOIN symbols s ON s.name_lc = m.name_lc
             JOIN files f ON f.content_hash = s.content_hash
             GROUP BY m.name_lc
             ORDER BY m.name_lc",
        )
        .bind(pattern)
        .bind(limit);

        if std::env::var("POINTER_EXPLAIN_SEARCH_SQL").is_ok() {
            let sql = format!("EXPLAIN (ANALYZE, VERBOSE, BUFFERS) {}", query.sql());
            if let Ok(Some(args)) = query.take_arguments() {
                let args: PgArguments = args;
                let explain_args = args.clone();
                match sqlx::query_scalar_with::<Postgres, String, PgArguments>(&sql, explain_args)
                    .fetch_all(&self.pool)
                    .await
                {
                    Ok(rows) => {
                        for line in rows {
                            tracing::info!(target: "pointer::autocomplete_sql", "{}", line);
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            target: "pointer::autocomplete_sql",
                            "failed to run EXPLAIN: {}",
                            err
                        );
                    }
                }
                query = sqlx::query_as_with::<Postgres, (String, String, String), PgArguments>(
                    query.sql(),
                    args,
                );
            }
        }

        let rows: Vec<(String, String, String)> = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| DbError::Database(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(name, repository, file_path)| SymbolSuggestion {
                name,
                repository,
                file_path,
            })
            .collect())
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
            (symbol.content_hash.clone(), symbol.name.clone())
        });

        for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
            let mut qb = QueryBuilder::new("INSERT INTO symbols (content_hash, name, name_lc) ");
            qb.push_values(chunk.iter().copied(), |mut b, symbol| {
                let name_lc = symbol.name.to_lowercase();
                b.push_bind(&symbol.content_hash)
                    .push_bind(&symbol.name)
                    .push_bind(name_lc);
            });
            qb.push(" ON CONFLICT (content_hash, name) DO NOTHING");

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
            let mut namespaces: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for reference in chunk.iter().copied() {
                let namespace = reference
                    .namespace
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("");
                namespaces.insert(namespace.to_string());
            }

            if !namespaces.is_empty() {
                let mut ns_qb = QueryBuilder::new("INSERT INTO symbol_namespaces (namespace) ");
                ns_qb.push_values(namespaces.iter(), |mut b, namespace| {
                    b.push_bind(namespace);
                });
                ns_qb.push(" ON CONFLICT (namespace) DO NOTHING");

                ns_qb
                    .build()
                    .execute(tx.as_mut())
                    .await
                    .map_err(|e| DbError::Database(e.to_string()))?;
            }

            let mut qb = QueryBuilder::new(
                "WITH data (content_hash, namespace, name, kind, line_number, column_number) AS (",
            );
            qb.push_values(chunk.iter().copied(), |mut b, reference| {
                let line: i32 = reference.line.try_into().unwrap_or(i32::MAX);
                let column: i32 = reference.column.try_into().unwrap_or(i32::MAX);
                let namespace = reference
                    .namespace
                    .as_deref()
                    .filter(|s| !s.is_empty())
                    .unwrap_or("");
                b.push_bind(&reference.content_hash)
                    .push_bind(namespace)
                    .push_bind(&reference.name)
                    .push_bind(&reference.kind)
                    .push_bind(line)
                    .push_bind(column);
            });
            qb.push(
                ") INSERT INTO symbol_references (symbol_id, namespace_id, kind, line_number, column_number) \
                 SELECT s.id, sn.id, data.kind, data.line_number, data.column_number \
                 FROM data \
                 JOIN symbols s \
                   ON s.content_hash = data.content_hash \
                  AND s.name = data.name \
                 JOIN symbol_namespaces sn \
                   ON sn.namespace = data.namespace \
                 ON CONFLICT (symbol_id, namespace_id, line_number, column_number, kind) DO NOTHING",
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
    #[allow(dead_code)]
    line_count: i32,
    content_text: String,
    match_line_number: i32,
    branches: Vec<String>,
    live_branches: Vec<String>,
    is_historical: bool,
    snapshot_indexed_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct RankedFileRow {
    repository: String,
    #[allow(dead_code)]
    commit_sha: String,
    file_path: String,
    #[allow(dead_code)]
    content_hash: String,
    #[allow(dead_code)]
    chunk_index: i32,
    #[allow(dead_code)]
    total_score: f64,
    #[allow(dead_code)]
    include_historical: bool,
    branches: Vec<String>,
    #[allow(dead_code)]
    live_branches: Vec<String>,
    #[allow(dead_code)]
    is_historical: bool,
    #[allow(dead_code)]
    snapshot_indexed_at: Option<DateTime<Utc>>,
}

#[derive(sqlx::FromRow)]
struct SymbolRow {
    #[allow(dead_code)]
    id: i32,
    symbol: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    language: Option<String>,
    repository: String,
    commit_sha: String,
    file_path: String,
    #[sqlx(rename = "line_number")]
    line: Option<i32>,
    #[sqlx(rename = "column_number")]
    column: Option<i32>,
    #[sqlx(rename = "score")]
    score: f64,
    references: Option<Json<Vec<ReferenceEntry>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct ReferenceEntry {
    namespace: Option<String>,
    name: String,
    kind: Option<String>,
    repository: String,
    commit_sha: String,
    file_path: String,
    line: Option<i32>,
    column: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct SnippetRow {
    idx: i32,
    line: i32,
    line_count: i32,
    start_line: i32,
    end_line: i32,
    snippet: Option<String>,
}

#[derive(Clone, Debug)]
struct FileAggregate {
    entries: Vec<SearchResultRow>,
}

const FACET_LIMIT: usize = 8;

fn snippet_signal_score(text: &str) -> (i32, i32, i32) {
    let mark_count = text.matches("<mark>").count() as i32;
    let exact_count = count_exact_mark_matches(text);
    let signal_count = text
        .bytes()
        .filter(|byte| matches!(byte, b':' | b'=' | b'(' | b')'))
        .count() as i32;
    (exact_count, mark_count, signal_count)
}

fn count_exact_mark_matches(text: &str) -> i32 {
    const OPEN: &str = "<mark>";
    const CLOSE: &str = "</mark>";
    let mut count = 0;
    let mut idx = 0;
    let bytes = text.as_bytes();

    while let Some(open_rel) = text[idx..].find(OPEN) {
        let open_idx = idx + open_rel;
        let content_start = open_idx + OPEN.len();
        let Some(close_rel) = text[content_start..].find(CLOSE) else {
            break;
        };
        let close_idx = content_start + close_rel;
        let before = if open_idx == 0 {
            None
        } else {
            bytes.get(open_idx.wrapping_sub(1)).copied()
        };
        let after_idx = close_idx + CLOSE.len();
        let after = bytes.get(after_idx).copied();

        let before_ident = before.map(is_identifier_byte).unwrap_or(false);
        let after_ident = after.map(is_identifier_byte).unwrap_or(false);
        if !before_ident && !after_ident {
            count += 1;
        }

        idx = after_idx;
    }

    count
}

fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn snippet_bounds(content_text: &str, match_line: i32) -> (i32, i32) {
    let lines: Vec<&str> = content_text.split('\n').collect();
    let line_count = lines.len().max(1) as i32;
    let match_offset = lines
        .iter()
        .position(|line| line.contains("<mark>"))
        .unwrap_or(0) as i32;
    let mut start_line = match_line.saturating_sub(match_offset);
    if start_line < 1 {
        start_line = 1;
    }
    let end_line = start_line.saturating_add(line_count.saturating_sub(1));
    (start_line, end_line)
}

fn merge_overlapping_snippets(mut snippets: Vec<SearchSnippet>) -> Vec<SearchSnippet> {
    if snippets.len() <= 1 {
        return snippets;
    }

    snippets.sort_by(|a, b| {
        a.start_line
            .cmp(&b.start_line)
            .then_with(|| a.end_line.cmp(&b.end_line))
    });

    let mut merged: Vec<SearchSnippet> = Vec::new();
    let mut current_start = snippets[0].start_line;
    let mut current_end = snippets[0].end_line;
    let mut current_match_line = snippets[0].match_line;
    let mut line_map = build_snippet_line_map(&snippets[0]);

    for snippet in snippets.into_iter().skip(1) {
        if snippet.start_line <= current_end.saturating_add(1) {
            current_end = current_end.max(snippet.end_line);
            merge_snippet_line_map(&mut line_map, &snippet);
        } else {
            merged.push(build_snippet_from_map(
                current_start,
                current_end,
                current_match_line,
                &line_map,
            ));
            current_start = snippet.start_line;
            current_end = snippet.end_line;
            current_match_line = snippet.match_line;
            line_map = build_snippet_line_map(&snippet);
        }
    }

    merged.push(build_snippet_from_map(
        current_start,
        current_end,
        current_match_line,
        &line_map,
    ));

    merged
}

fn build_snippet_line_map(snippet: &SearchSnippet) -> BTreeMap<i32, (String, i32)> {
    let mut map = BTreeMap::new();
    for (idx, line) in snippet.content_text.split('\n').enumerate() {
        let line_number = snippet.start_line.saturating_add(idx as i32);
        insert_snippet_line(&mut map, line_number, line);
    }
    map
}

fn merge_snippet_line_map(map: &mut BTreeMap<i32, (String, i32)>, snippet: &SearchSnippet) {
    for (idx, line) in snippet.content_text.split('\n').enumerate() {
        let line_number = snippet.start_line.saturating_add(idx as i32);
        insert_snippet_line(map, line_number, line);
    }
}

fn insert_snippet_line(map: &mut BTreeMap<i32, (String, i32)>, line: i32, text: &str) {
    let mark_count = text.matches("<mark>").count() as i32;
    match map.get(&line) {
        Some((_, existing_marks)) if *existing_marks >= mark_count => {}
        _ => {
            map.insert(line, (text.to_string(), mark_count));
        }
    }
}

fn build_snippet_from_map(
    start_line: i32,
    end_line: i32,
    match_line: i32,
    map: &BTreeMap<i32, (String, i32)>,
) -> SearchSnippet {
    let mut lines = Vec::new();
    for line_number in start_line..=end_line {
        if let Some((line, _)) = map.get(&line_number) {
            lines.push(line.clone());
        } else {
            lines.push(String::new());
        }
    }

    SearchSnippet {
        start_line,
        end_line,
        match_line,
        content_text: lines.join("\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_phase1_sql(request: &TextSearchRequest) -> String {
        let page_index = u64::from(request.page);
        let page_size = u64::from(request.page_size.max(1));
        let sample_factor = u64::from(FILE_SAMPLE_FACTOR.max(1));
        let base_limit = page_index
            .saturating_add(1)
            .saturating_mul(page_size)
            .saturating_mul(sample_factor);
        let minimum = page_size.saturating_mul(sample_factor);
        let fetch_limit_u64 = base_limit.max(minimum).saturating_add(1);
        let mut fetch_limit = fetch_limit_u64.min(i64::MAX as u64) as i64;
        fetch_limit = fetch_limit.min(5000);
        let file_limit = fetch_limit.min(25000);
        let plan_row_limit: i64 = 5000;

        let needs_live_branch_filter = request
            .plans
            .iter()
            .any(|plan| plan.branches.is_empty() && !plan.include_historical);

        let symbol_terms: Vec<String> = collect_symbol_terms(request)
            .into_iter()
            .map(|t| t.to_lowercase())
            .collect();

        let mut qb = QueryBuilder::new("");
        push_search_ctes(
            &mut qb,
            request,
            plan_row_limit,
            fetch_limit,
            file_limit,
            needs_live_branch_filter,
            &symbol_terms,
        );
        qb.sql().to_string()
    }

    #[test]
    fn snippet_bounds_use_match_offset() {
        let text = "alpha\n<mark>ip_rcv</mark>\nomega";
        let (start, end) = snippet_bounds(text, 100);
        assert_eq!(start, 99);
        assert_eq!(end, 101);
    }

    #[test]
    fn merge_overlapping_snippets_merges_adjacent_and_preserves_marks() {
        let snippet_a = SearchSnippet {
            start_line: 10,
            end_line: 12,
            match_line: 11,
            content_text: "line10\n<mark>hit_a</mark>\nline12".to_string(),
        };
        let snippet_b = SearchSnippet {
            start_line: 13,
            end_line: 14,
            match_line: 13,
            content_text: "<mark>hit_b</mark>\nline14".to_string(),
        };

        let merged = merge_overlapping_snippets(vec![snippet_a, snippet_b]);
        assert_eq!(merged.len(), 1);
        let merged_snippet = &merged[0];
        assert_eq!(merged_snippet.start_line, 10);
        assert_eq!(merged_snippet.end_line, 14);
        assert_eq!(merged_snippet.match_line, 11);
        let lines: Vec<&str> = merged_snippet.content_text.split('\n').collect();
        assert_eq!(lines.len(), 5);
        assert!(merged_snippet.content_text.contains("<mark>hit_a</mark>"));
        assert!(merged_snippet.content_text.contains("<mark>hit_b</mark>"));
    }

    #[test]
    fn merge_overlapping_snippets_prefers_more_marks_on_overlap() {
        let snippet_a = SearchSnippet {
            start_line: 10,
            end_line: 12,
            match_line: 11,
            content_text: "line10\nline11\nline12".to_string(),
        };
        let snippet_b = SearchSnippet {
            start_line: 12,
            end_line: 14,
            match_line: 12,
            content_text: "<mark>hit_b</mark>\nline13\nline14".to_string(),
        };

        let merged = merge_overlapping_snippets(vec![snippet_a, snippet_b]);
        assert_eq!(merged.len(), 1);
        let merged_snippet = &merged[0];
        assert_eq!(merged_snippet.start_line, 10);
        assert_eq!(merged_snippet.end_line, 14);
        let lines: Vec<&str> = merged_snippet.content_text.split('\n').collect();
        assert_eq!(lines.len(), 5);
        assert_eq!(lines[2], "<mark>hit_b</mark>");
    }

    #[test]
    fn multi_term_search_builds_intersect_filter() {
        let request = TextSearchRequest::from_query_str("polly LinkAllPasses").unwrap();
        let sql = build_phase1_sql(&request);
        assert!(sql.contains("files.content_hash IN"));
        assert!(sql.contains("INTERSECT"));
    }

    #[test]
    fn single_term_search_omits_intersect_filter() {
        let request = TextSearchRequest::from_query_str("polly").unwrap();
        let sql = build_phase1_sql(&request);
        assert!(!sql.contains("INTERSECT"));
    }
}

fn build_search_stats(rows: &[RankedFileRow]) -> SearchResultsStats {
    let mut directory_counts: HashMap<String, u32> = HashMap::new();
    let mut repository_counts: HashMap<String, u32> = HashMap::new();
    let mut branch_counts: HashMap<String, u32> = HashMap::new();

    for row in rows {
        if let Some(directory) = parent_directory(&row.file_path) {
            *directory_counts.entry(directory).or_insert(0) += 1;
        }
        *repository_counts.entry(row.repository.clone()).or_insert(0) += 1;

        if !row.branches.is_empty() {
            let unique_branches: HashSet<&String> = row.branches.iter().collect();
            for branch in unique_branches {
                *branch_counts.entry(branch.clone()).or_insert(0) += 1;
            }
        }
    }

    SearchResultsStats {
        common_directories: map_to_facets(directory_counts, FACET_LIMIT),
        top_repositories: map_to_facets(repository_counts, FACET_LIMIT),
        top_branches: map_to_facets(branch_counts, FACET_LIMIT),
    }
}

fn map_to_facets(counts: HashMap<String, u32>, limit: usize) -> Vec<FacetCount> {
    let mut items: Vec<(String, u32)> = counts.into_iter().collect();
    items.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.to_lowercase().cmp(&b.0.to_lowercase()))
            .then_with(|| a.0.cmp(&b.0))
    });
    items
        .into_iter()
        .take(limit)
        .map(|(value, count)| FacetCount { value, count })
        .collect()
}

fn parent_directory(path: &str) -> Option<String> {
    path.rsplit_once('/').map(|(dir, _)| dir.to_string())
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

fn collect_symbol_terms(request: &TextSearchRequest) -> HashSet<String> {
    let mut terms = HashSet::new();
    for plan in &request.plans {
        for predicate in &plan.required_terms {
            if let ContentPredicate::Plain(value) = predicate {
                if !value.is_empty() && looks_like_symbol(value) {
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

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct FileGroupKey {
    repository: String,
    commit_sha: String,
    file_path: String,
    content_hash: String,
}

fn split_fully_qualified(value: &str) -> (Option<String>, String) {
    if let Some(idx) = value.rfind("::") {
        let (ns, name) = value.split_at(idx);
        let name = name.trim_start_matches("::").to_string();
        let namespace = if ns.is_empty() {
            None
        } else {
            Some(ns.to_string())
        };
        (namespace, name)
    } else {
        (None, value.to_string())
    }
}
