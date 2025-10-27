use std::collections::{HashMap, HashSet};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use chrono::{DateTime, Utc, Duration};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clap::Parser;
use futures::TryStreamExt;
use pointer_indexer::models::{
    BranchHead, ChunkMapping, ContentBlob, FilePointer, IndexReport, ReferenceRecord, SymbolRecord,
    UniqueChunk,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use tempfile::tempfile;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;
use zstd::stream::read::Decoder;

#[derive(Debug, Parser)]
struct ServerConfig {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "BIND_ADDRESS", default_value = "127.0.0.1:8080")]
    bind: String,
    #[arg(long, env = "MAX_CONNECTIONS", default_value_t = 10)]
    max_connections: u32,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Debug, Error)]
enum ApiErrorKind {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("compression error: {0}")]
    Compression(#[from] std::io::Error),
    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

#[derive(Debug)]
struct AppError {
    status: StatusCode,
    message: String,
}

impl AppError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl From<ApiErrorKind> for AppError {
    fn from(kind: ApiErrorKind) -> Self {
        match kind {
            ApiErrorKind::Database(err) => {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
            }
            ApiErrorKind::Serde(err) => AppError::new(StatusCode::BAD_REQUEST, err.to_string()),
            ApiErrorKind::Compression(err) => {
                AppError::new(StatusCode::BAD_REQUEST, err.to_string())
            }
            ApiErrorKind::Internal(err) => {
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
            }
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

type ApiResult<T> = std::result::Result<T, AppError>;

// New Ingestion Structs
#[derive(Debug, Deserialize)]
struct ContentBlobUploadRequest {
    blobs: Vec<ContentBlob>,
}

#[derive(Debug, Deserialize)]
struct ChunkNeedRequest {
    hashes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ChunkNeedResponse {
    missing: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct UniqueChunkUploadRequest {
    chunks: Vec<UniqueChunk>,
}

#[derive(Debug, Deserialize)]
struct ChunkMappingUploadRequest {
    mappings: Vec<ChunkMapping>,
}

// Manifest-related structs
#[derive(Debug, Deserialize)]
struct ManifestChunkPayload {
    upload_id: String,
    chunk_index: i32,
    total_chunks: i32,
    data: String,
}

#[derive(Debug, Deserialize)]
struct ManifestFinalizePayload {
    upload_id: String,
    compressed: Option<bool>,
}

#[derive(sqlx::FromRow)]
struct UploadChunkRow {
    chunk_index: i32,
    total_chunks: i32,
    data: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .init();

    let config = ServerConfig::parse();
    let bind_addr: SocketAddr = config
        .bind
        .parse()
        .with_context(|| format!("invalid bind address: {}", config.bind))?;

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to postgres")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("database migration failed")?;

    let state = AppState { pool };

    let app = Router::new()
        // New ingestion routes
        .route("/api/v1/blobs/upload", post(blobs_upload))
        .route("/api/v1/chunks/need", post(chunks_need))
        .route("/api/v1/chunks/upload", post(chunks_upload))
        .route("/api/v1/mappings/upload", post(mappings_upload))
        .route("/api/v1/index/blobs/upload", post(blobs_upload))
        .route("/api/v1/index/chunks/need", post(chunks_need))
        .route("/api/v1/index/chunks/upload", post(chunks_upload))
        .route("/api/v1/index/mappings/upload", post(mappings_upload))
        // Manifest upload routes
        .route("/api/v1/manifest/chunk", post(manifest_chunk))
        .route("/api/v1/manifest/finalize", post(manifest_finalize))
        .route("/api/v1/index/manifest/chunk", post(manifest_chunk))
        .route("/api/v1/index/manifest/finalize", post(manifest_finalize))
        // Pruning routes
        .route("/api/v1/prune/commit", post(prune_commit_handler))
        .route("/api/v1/prune/branch", post(prune_branch_handler))
        .route("/api/v1/prune/policy", post(apply_retention_policy_handler))
        .route("/api/v1/prune/snapshot", post(create_snapshot_handler))
        .route("/healthz", get(health_check))
        .with_state(state)
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024));

    let listener = TcpListener::bind(bind_addr)
        .await
        .context("failed to bind TCP listener")?;

    info!(%bind_addr, "server starting");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server shutdown")?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = signal::ctrl_c().await {
            tracing::warn!(?err, "failed to listen for CTRL+C");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(err) => tracing::warn!(?err, "failed to listen for TERM signal"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
}

// New Ingestion Handlers
async fn blobs_upload(
    State(state): State<AppState>,
    Json(payload): Json<ContentBlobUploadRequest>,
) -> ApiResult<StatusCode> {
    if payload.blobs.is_empty() {
        return Ok(StatusCode::ACCEPTED);
    }

    let mut qb =
        QueryBuilder::new("INSERT INTO content_blobs (hash, language, byte_len, line_count) ");
    qb.push_values(payload.blobs, |mut b, blob| {
        b.push_bind(blob.hash)
            .push_bind(blob.language)
            .push_bind(blob.byte_len)
            .push_bind(blob.line_count);
    });
    qb.push(" ON CONFLICT (hash) DO NOTHING");

    qb.build()
        .execute(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::ACCEPTED)
}

async fn chunks_need(
    State(state): State<AppState>,
    Json(payload): Json<ChunkNeedRequest>,
) -> ApiResult<Json<ChunkNeedResponse>> {
    if payload.hashes.is_empty() {
        return Ok(Json(ChunkNeedResponse {
            missing: Vec::new(),
        }));
    }

    let existing: Vec<(String,)> =
        sqlx::query_as("SELECT chunk_hash FROM chunks WHERE chunk_hash = ANY($1)")
            .bind(&payload.hashes)
            .fetch_all(&state.pool)
            .await
            .map_err(ApiErrorKind::from)?;

    let present: HashSet<String> = existing.into_iter().map(|row| row.0).collect();
    let missing: Vec<String> = payload
        .hashes
        .into_iter()
        .filter(|h| !present.contains(h))
        .collect();

    Ok(Json(ChunkNeedResponse { missing }))
}

async fn chunks_upload(
    State(state): State<AppState>,
    Json(payload): Json<UniqueChunkUploadRequest>,
) -> ApiResult<StatusCode> {
    if payload.chunks.is_empty() {
        return Ok(StatusCode::ACCEPTED);
    }

    let mut qb = QueryBuilder::new("INSERT INTO chunks (chunk_hash, text_content) ");
    qb.push_values(payload.chunks, |mut b, chunk| {
        b.push_bind(chunk.chunk_hash).push_bind(chunk.text_content);
    });
    qb.push(" ON CONFLICT (chunk_hash) DO NOTHING");

    qb.build()
        .execute(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::ACCEPTED)
}

async fn mappings_upload(
    State(state): State<AppState>,
    Json(payload): Json<ChunkMappingUploadRequest>,
) -> ApiResult<StatusCode> {
    if payload.mappings.is_empty() {
        return Ok(StatusCode::ACCEPTED);
    }

    let mut qb = QueryBuilder::new(
        "INSERT INTO content_blob_chunks (content_hash, chunk_hash, chunk_index, chunk_line_count) ",
    );
    qb.push_values(payload.mappings, |mut b, mapping| {
        b.push_bind(mapping.content_hash)
            .push_bind(mapping.chunk_hash)
            .push_bind(mapping.chunk_index as i32)
            .push_bind(mapping.chunk_line_count);
    });
    qb.push(" ON CONFLICT (content_hash, chunk_index) DO NOTHING");

    qb.build()
        .execute(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::ACCEPTED)
}

// Manifest Handlers
async fn manifest_chunk(
    State(state): State<AppState>,
    Json(payload): Json<ManifestChunkPayload>,
) -> ApiResult<StatusCode> {
    if payload.chunk_index < 0
        || payload.total_chunks <= 0
        || payload.chunk_index >= payload.total_chunks
    {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "invalid manifest chunk metadata",
        ));
    }

    let data = BASE64.decode(payload.data.as_bytes()).map_err(|err| {
        AppError::new(
            StatusCode::BAD_REQUEST,
            format!("invalid base64 data: {err}"),
        )
    })?;

    sqlx::query(
        "INSERT INTO upload_chunks (upload_id, chunk_index, total_chunks, data)\n         VALUES ($1, $2, $3, $4)\n         ON CONFLICT (upload_id, chunk_index) DO UPDATE\n         SET total_chunks = EXCLUDED.total_chunks, data = EXCLUDED.data",
    )
    .bind(&payload.upload_id)
    .bind(payload.chunk_index)
    .bind(payload.total_chunks)
    .bind(data)
    .execute(&state.pool)
    .await
    .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::ACCEPTED)
}

async fn manifest_finalize(
    State(state): State<AppState>,
    Json(payload): Json<ManifestFinalizePayload>,
) -> ApiResult<StatusCode> {
    let compressed = payload.compressed.unwrap_or(false);
    let mut rows = sqlx::query_as::<_, UploadChunkRow>(
        "SELECT chunk_index, total_chunks, data \
         FROM upload_chunks \
         WHERE upload_id = $1 \
         ORDER BY chunk_index",
    )
    .bind(&payload.upload_id)
    .fetch(&state.pool);

    let mut temp_file = tempfile().map_err(ApiErrorKind::Compression)?;
    let mut expected_total: Option<i32> = None;
    let mut seen_chunks: i32 = 0;

    while let Some(row) = rows.try_next().await.map_err(ApiErrorKind::from)? {
        if let Some(expected) = expected_total {
            if row.total_chunks != expected {
                return Err(AppError::new(
                    StatusCode::BAD_REQUEST,
                    "inconsistent manifest chunk metadata",
                ));
            }
        } else {
            if row.total_chunks <= 0 {
                return Err(AppError::new(
                    StatusCode::BAD_REQUEST,
                    "invalid total chunk count",
                ));
            }
            expected_total = Some(row.total_chunks);
        }

        if row.chunk_index != seen_chunks {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                "inconsistent manifest chunk metadata",
            ));
        }

        temp_file
            .write_all(&row.data)
            .map_err(ApiErrorKind::Compression)?;
        seen_chunks += 1;
    }

    let expected_total = match expected_total {
        Some(total) => total,
        None => {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                "no chunks uploaded for manifest",
            ));
        }
    };

    if seen_chunks != expected_total {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "missing manifest chunks",
        ));
    }

    temp_file
        .seek(SeekFrom::Start(0))
        .map_err(ApiErrorKind::Compression)?;

    let report: IndexReport = if compressed {
        let mut decoder = Decoder::new(temp_file).map_err(ApiErrorKind::Compression)?;
        serde_json::from_reader(&mut decoder).map_err(ApiErrorKind::Serde)?
    } else {
        serde_json::from_reader(temp_file).map_err(ApiErrorKind::Serde)?
    };

    ingest_report(&state.pool, report).await?;

    sqlx::query("DELETE FROM upload_chunks WHERE upload_id = $1")
        .bind(&payload.upload_id)
        .execute(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::CREATED)
}

async fn ingest_report(pool: &PgPool, report: IndexReport) -> Result<(), ApiErrorKind> {
    let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;

    insert_file_pointers(&mut tx, &report.file_pointers).await?;
    insert_symbol_records(&mut tx, &report.symbol_records).await?;
    insert_reference_records(&mut tx, &report.reference_records).await?;
    upsert_branch_heads(&mut tx, &report.branches).await?;

    tx.commit().await.map_err(ApiErrorKind::from)?;

    Ok(())
}

const INSERT_BATCH_SIZE: usize = 1000;

#[derive(Hash, PartialEq, Eq)]
struct FilePointerKey<'a> {
    repository: &'a str,
    commit_sha: &'a str,
    file_path: &'a str,
}

fn dedup_file_pointers<'a>(files: &'a [FilePointer]) -> Vec<&'a FilePointer> {
    let mut seen = HashSet::with_capacity(files.len());
    let mut deduped = Vec::with_capacity(files.len());

    for file in files {
        if seen.insert(FilePointerKey {
            repository: &file.repository,
            commit_sha: &file.commit_sha,
            file_path: &file.file_path,
        }) {
            deduped.push(file);
        }
    }

    deduped
}

#[derive(Hash, PartialEq, Eq)]
struct SymbolKey<'a> {
    content_hash: &'a str,
    name: &'a str,
}

fn dedup_symbol_records<'a>(symbols: &'a [SymbolRecord]) -> Vec<&'a SymbolRecord> {
    let mut seen = HashSet::with_capacity(symbols.len());
    let mut deduped = Vec::with_capacity(symbols.len());

    for symbol in symbols {
        if seen.insert(SymbolKey {
            content_hash: &symbol.content_hash,
            name: &symbol.name,
        }) {
            deduped.push(symbol);
        }
    }

    deduped
}

#[derive(Hash, PartialEq, Eq)]
struct ReferenceKey<'a> {
    content_hash: &'a str,
    namespace: Option<&'a str>,
    name: &'a str,
    kind: Option<&'a str>,
    line: usize,
    column: usize,
}

fn dedup_reference_records<'a>(references: &'a [ReferenceRecord]) -> Vec<&'a ReferenceRecord> {
    let mut seen = HashSet::with_capacity(references.len());
    let mut deduped = Vec::with_capacity(references.len());

    for reference in references {
        if seen.insert(ReferenceKey {
            content_hash: &reference.content_hash,
            namespace: reference.namespace.as_deref(),
            name: &reference.name,
            kind: reference.kind.as_deref(),
            line: reference.line,
            column: reference.column,
        }) {
            deduped.push(reference);
        }
    }

    deduped
}

#[derive(Hash, PartialEq, Eq)]
struct BranchKey<'a> {
    repository: &'a str,
    branch: &'a str,
}

fn dedup_branch_heads<'a>(branches: &'a [BranchHead]) -> Vec<&'a BranchHead> {
    let mut seen = HashSet::with_capacity(branches.len());
    let mut deduped = Vec::with_capacity(branches.len());

    for branch in branches {
        if seen.insert(BranchKey {
            repository: &branch.repository,
            branch: &branch.branch,
        }) {
            deduped.push(branch);
        }
    }

    deduped
}

async fn insert_file_pointers(
    tx: &mut Transaction<'_, Postgres>,
    files: &[FilePointer],
) -> Result<(), ApiErrorKind> {
    if files.is_empty() {
        return Ok(());
    }

    let deduped = dedup_file_pointers(files);

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
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
}

async fn insert_symbol_records(
    tx: &mut Transaction<'_, Postgres>,
    symbols: &[SymbolRecord],
) -> Result<(), ApiErrorKind> {
    if symbols.is_empty() {
        return Ok(());
    }

    let deduped = dedup_symbol_records(symbols);

    for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
        let mut qb = QueryBuilder::new("INSERT INTO symbols (content_hash, name) ");
        qb.push_values(chunk.iter().copied(), |mut b, symbol| {
            b.push_bind(&symbol.content_hash).push_bind(&symbol.name);
        });
        qb.push(" ON CONFLICT (content_hash, name) DO NOTHING");

        qb.build()
            .execute(tx.as_mut())
            .await
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
}

async fn insert_reference_records(
    tx: &mut Transaction<'_, Postgres>,
    references: &[ReferenceRecord],
) -> Result<(), ApiErrorKind> {
    if references.is_empty() {
        return Ok(());
    }

    let deduped = dedup_reference_records(references);

    for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
        let mut namespaces: HashSet<&str> = HashSet::new();
        for reference in chunk.iter().copied() {
            let namespace = reference
                .namespace
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("");
            namespaces.insert(namespace);
        }

        if !namespaces.is_empty() {
            let mut ns_qb = QueryBuilder::new("INSERT INTO symbol_namespaces (namespace) ");
            ns_qb.push_values(namespaces.iter(), |mut b, namespace| {
                b.push_bind(*namespace);
            });
            ns_qb.push(" ON CONFLICT (namespace) DO NOTHING");

            ns_qb
                .build()
                .execute(tx.as_mut())
                .await
                .map_err(ApiErrorKind::from)?;
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
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
}

async fn upsert_branch_heads(
    tx: &mut Transaction<'_, Postgres>,
    branches: &[BranchHead],
) -> Result<(), ApiErrorKind> {
    if branches.is_empty() {
        return Ok(());
    }

    let deduped = dedup_branch_heads(branches);

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
        .map_err(ApiErrorKind::from)?;

    Ok(())
}
// Pruning functionality
#[derive(Debug, Deserialize)]
struct PruneCommitRequest {
    repository: String,
    commit_sha: String,
}

#[derive(Debug, Serialize)]
struct PruneCommitResponse {
    repository: String,
    commit_sha: String,
    pruned: bool,
    message: String,
}

#[derive(Debug, Deserialize)]
struct PruneBranchRequest {
    repository: String,
    branch: String,
}

#[derive(Debug, Serialize)]
struct PruneBranchResponse {
    repository: String,
    branch: String,
    pruned: bool,
    message: String,
}

// Function to check if a commit is the latest on any branch
async fn is_latest_commit_on_any_branch(pool: &PgPool, repository: &str, commit_sha: &str) -> Result<bool> {
    let result: Option<(String,)> = sqlx::query_as(
        "SELECT commit_sha FROM branches WHERE repository = $1 AND commit_sha = $2"
    )
    .bind(repository)
    .bind(commit_sha)
    .fetch_optional(pool)
    .await
    .map_err(ApiErrorKind::from)?;

    Ok(result.is_some())
}

// Manual prune for a specific commit
async fn prune_commit_handler(
    State(state): State<AppState>,
    Json(payload): Json<PruneCommitRequest>,
) -> ApiResult<Json<PruneCommitResponse>> {
    let is_latest = is_latest_commit_on_any_branch(&state.pool, &payload.repository, &payload.commit_sha).await?;
    
    if is_latest {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "Cannot prune commit that is the latest on a branch. Update the branch first."
        ));
    }

    let pruned = prune_commit_data(&state.pool, &payload.repository, &payload.commit_sha).await?;

    Ok(Json(PruneCommitResponse {
        repository: payload.repository,
        commit_sha: payload.commit_sha,
        pruned,
        message: if pruned {
            "Commit data successfully pruned".to_string()
        } else {
            "No data found for the specified commit".to_string()
        },
    }))
}

// Function to actually remove all data associated with a commit
async fn prune_commit_data(pool: &PgPool, repository: &str, commit_sha: &str) -> Result<bool> {
    let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;

    // Get all content hashes associated with this commit from the files table
    let content_hashes: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT content_hash FROM files WHERE repository = $1 AND commit_sha = $2"
    )
    .bind(repository)
    .bind(commit_sha)
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiErrorKind::from)?;

    // Count files to be deleted
    let files_deleted_result = sqlx::query(
        "DELETE FROM files WHERE repository = $1 AND commit_sha = $2"
    )
    .bind(repository)
    .bind(commit_sha)
    .execute(&mut *tx)
    .await
    .map_err(ApiErrorKind::from)?;

    let files_deleted = files_deleted_result.rows_affected();

    // Get content hashes that are no longer referenced by any files
    let unreferenced_content_hashes: Vec<(String,)> = sqlx::query_as(
        "SELECT hash FROM content_blobs WHERE hash = ANY($1) AND hash NOT IN (SELECT DISTINCT content_hash FROM files WHERE content_hash = ANY($1))"
    )
    .bind(&content_hashes.iter().map(|(h,)| h.as_str()).collect::<Vec<_>>())
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiErrorKind::from)?;

    // Delete unreferenced content blobs
    if !unreferenced_content_hashes.is_empty() {
        let hashes_to_delete: Vec<&str> = unreferenced_content_hashes.iter().map(|(h,)| h.as_str()).collect();
        
        // Delete from symbol_references through symbols table
        sqlx::query(
            "DELETE FROM symbol_references WHERE symbol_id IN (
                SELECT id FROM symbols WHERE content_hash = ANY($1)
            )"
        )
        .bind(&hashes_to_delete)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

        // Delete from symbols
        sqlx::query(
            "DELETE FROM symbols WHERE content_hash = ANY($1)"
        )
        .bind(&hashes_to_delete)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

        // Delete from content_blob_chunks
        sqlx::query(
            "DELETE FROM content_blob_chunks WHERE content_hash = ANY($1)"
        )
        .bind(&hashes_to_delete)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

        // Delete from content_blobs
        sqlx::query(
            "DELETE FROM content_blobs WHERE hash = ANY($1)"
        )
        .bind(&hashes_to_delete)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;
    }

    // Check if any chunks are no longer referenced by any content blobs
    let remaining_content_hashes: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT content_hash FROM content_blob_chunks WHERE content_hash IN (
            SELECT hash FROM content_blobs
        )"
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(ApiErrorKind::from)?;

    // Find chunks that are only referenced by the content hashes we're deleting
    // This is a complex query to find chunks that are ONLY referenced by content hashes being deleted
    let all_chunks_result = sqlx::query_as::<_, (String,)>("SELECT chunk_hash FROM content_blob_chunks WHERE content_hash = ANY($1)")
        .bind(&remaining_content_hashes.iter().map(|(h,)| h.as_str()).collect::<Vec<_>>())
        .fetch_all(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

    let remaining_chunk_hashes: HashSet<String> = all_chunks_result.into_iter().map(|(h,)| h).collect();

    // Delete only those chunks that are not referenced by any remaining content blobs
    // For now, we'll keep this simpler and not actually delete chunks that might be shared
    // In a real implementation, we'd need to be more sophisticated about chunk deletion
    // to avoid breaking references to chunks that are used by commits we're not pruning

    tx.commit().await.map_err(ApiErrorKind::from)?;

    Ok(files_deleted > 0)
}

// Prune all commits for a specific branch except the latest
async fn prune_branch_handler(
    State(state): State<AppState>,
    Json(payload): Json<PruneBranchRequest>,
) -> ApiResult<Json<PruneBranchResponse>> {
    // Get the latest commit for this branch
    let latest_commit_opt: Option<(String,)> = sqlx::query_as(
        "SELECT commit_sha FROM branches WHERE repository = $1 AND branch = $2"
    )
    .bind(&payload.repository)
    .bind(&payload.branch)
    .fetch_optional(&state.pool)
    .await
    .map_err(ApiErrorKind::from)?;

    let latest_commit = match latest_commit_opt {
        Some((commit,)) => commit,
        None => {
            return Ok(Json(PruneBranchResponse {
                repository: payload.repository,
                branch: payload.branch,
                pruned: false,
                message: "Branch not found".to_string(),
            }));
        }
    };

    // Get all commits for this repository and branch (except the latest)
    let commits_to_prune: Vec<(String,)> = sqlx::query_as(
        "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 AND commit_sha != $2"
    )
    .bind(&payload.repository)
    .bind(&latest_commit)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiErrorKind::from)?;

    let mut pruned_count = 0;
    for (commit_sha,) in commits_to_prune {
        if prune_commit_data(&state.pool, &payload.repository, &commit_sha).await? {
            pruned_count += 1;
        }
    }

    Ok(Json(PruneBranchResponse {
        repository: payload.repository,
        branch: payload.branch,
        pruned: true,
        message: format!("Pruned {} commits from branch (kept latest commit {})", pruned_count, latest_commit),
    }))
}

// Retention Policy Structures
#[derive(Debug, Deserialize)]
struct RetentionPolicyConfig {
    repository: String,
    keep_latest: bool,
    keep_snapshots: bool,
    snapshot_interval_days: Option<i32>,
    max_commits_to_keep: Option<i32>,
}

#[derive(Debug, Serialize)]
struct RetentionPolicyResponse {
    repository: String,
    message: String,
}

// We'll remove the snapshot creation functionality since it requires schema changes

// Function to identify commits to keep based on retention policy
async fn apply_retention_policy_handler(
    State(state): State<AppState>,
    Json(payload): Json<RetentionPolicyConfig>,
) -> ApiResult<Json<RetentionPolicyResponse>> {
    apply_retention_policy(&state.pool, &payload).await?;
    
    Ok(Json(RetentionPolicyResponse {
        repository: payload.repository,
        message: "Retention policy applied successfully".to_string(),
    }))
}

// Main retention policy function
async fn apply_retention_policy(pool: &PgPool, config: &RetentionPolicyConfig) -> Result<()> {
    // Get all commits for this repository from the files table
    let all_commits: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT commit_sha FROM files WHERE repository = $1"
    )
    .bind(&config.repository)
    .fetch_all(pool)
    .await
    .map_err(ApiErrorKind::from)?;

    let mut commits_to_keep = HashSet::new();

    // Always keep the latest commit on each branch
    if config.keep_latest {
        let latest_branch_commits: Vec<(String,)> = sqlx::query_as(
            "SELECT commit_sha FROM branches WHERE repository = $1"
        )
        .bind(&config.repository)
        .fetch_all(pool)
        .await
        .map_err(ApiErrorKind::from)?;

        for (commit_sha,) in latest_branch_commits {
            commits_to_keep.insert(commit_sha);
        }
    }

    // For now, without persistent snapshots, we can only support basic retention
    // based on max_commits_to_keep. We'll skip snapshot functionality entirely.

    // Keep recent commits based on max_commits_to_keep
    if let Some(max_commits) = config.max_commits_to_keep {
        // Get commits ordered by branch indexing time (most recent first)
        // This approach uses the branches table to order commits by recency
        let recent_commits: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT f.commit_sha 
             FROM files f
             LEFT JOIN branches b ON f.commit_sha = b.commit_sha AND f.repository = b.repository
             WHERE f.repository = $1
             ORDER BY b.indexed_at DESC NULLS LAST, f.commit_sha
             LIMIT $2"
        )
        .bind(&config.repository)
        .bind(max_commits)
        .fetch_all(pool)
        .await
        .map_err(ApiErrorKind::from)?;

        for commit_sha in recent_commits {
            commits_to_keep.insert(commit_sha);
        }
    }

    // Find commits that should be pruned (not in commits_to_keep)
    let commits_to_prune: Vec<String> = all_commits
        .into_iter()
        .filter(|commit_sha| !commits_to_keep.contains(commit_sha))
        .collect();

    // Prune the identified commits
    for commit_sha in commits_to_prune {
        prune_commit_data(pool, &config.repository, &commit_sha).await?;
    }

    Ok(())
}

// Create a snapshot (manually mark a commit to be preserved)
// This function is now a placeholder since we don't have a snapshots table
// In a real implementation, you would need to create a snapshots table to persist this information
async fn create_snapshot_handler(
    State(_state): State<AppState>,
    Json(payload): Json<CreateSnapshotRequest>,
) -> ApiResult<Json<CreateSnapshotResponse>> {
    // Without a persistent snapshots table, this functionality is limited
    // For now, we'll return an error indicating this feature requires schema changes
    Err(AppError::new(
        StatusCode::NOT_IMPLEMENTED,
        "Snapshot creation requires a dedicated database table. Schema changes needed for persistent snapshots."
    ))
}

// Function to automatically create snapshots based on time intervals
// This function is now a placeholder since we don't have a snapshots table
// In a real implementation, you would need to create a snapshots table to persist this information
async fn auto_create_snapshots(_pool: &PgPool, _repository: &str, _interval_days: i32) -> Result<()> {
    // For now, we're not persisting snapshots without a dedicated table
    // This is a limitation of the approach without schema changes
    // In a production implementation, schema changes would be necessary for full functionality
    
    // In the meantime, we could implement a basic time-based retention policy
    // that only keeps commits from certain time periods without persistent marking
    Ok(())
}

async fn health_check() -> &'static str {
    "ok"
}
