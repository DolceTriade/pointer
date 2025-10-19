use std::collections::HashSet;
use std::io::{Cursor, Read};
use std::net::SocketAddr;

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
use pointer_indexer::models::{
    BranchHead, ChunkMapping, ContentBlob, FilePointer, IndexReport, ReferenceRecord, SymbolRecord,
    UniqueChunk,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
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
    let rows: Vec<UploadChunkRow> = sqlx::query_as(
        "SELECT chunk_index, total_chunks, data FROM upload_chunks WHERE upload_id = $1 ORDER BY chunk_index",
    )
    .bind(&payload.upload_id)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiErrorKind::from)?;

    if rows.is_empty() {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "no chunks uploaded for manifest",
        ));
    }

    let expected_total = rows[0].total_chunks;
    if expected_total <= 0 {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "invalid total chunk count",
        ));
    }

    if rows.len() != expected_total as usize {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "missing manifest chunks",
        ));
    }

    for (index, row) in rows.iter().enumerate() {
        if row.chunk_index != index as i32 || row.total_chunks != expected_total {
            return Err(AppError::new(
                StatusCode::BAD_REQUEST,
                "inconsistent manifest chunk metadata",
            ));
        }
    }

    let mut combined = Vec::with_capacity(rows.iter().map(|row| row.data.len()).sum());
    for row in rows {
        combined.extend_from_slice(&row.data);
    }

    let compressed = payload.compressed.unwrap_or(false);
    let report_bytes = if compressed {
        let mut decoder = Decoder::new(Cursor::new(combined)).map_err(ApiErrorKind::Compression)?;
        let mut buf = Vec::new();
        decoder
            .read_to_end(&mut buf)
            .map_err(ApiErrorKind::Compression)?;
        buf
    } else {
        combined
    };

    let report: IndexReport = serde_json::from_slice(&report_bytes).map_err(ApiErrorKind::Serde)?;

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

fn dedup_by_key<'a, T, K, F>(items: &'a [T], mut key: F) -> Vec<&'a T>
where
    K: Eq + std::hash::Hash,
    F: FnMut(&'a T) -> K,
{
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(items.len());

    for item in items {
        if seen.insert(key(item)) {
            deduped.push(item);
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

    let deduped = dedup_by_key(symbols, |symbol| {
        (symbol.content_hash.clone(), symbol.name.clone())
    });

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
        let mut namespaces: std::collections::HashSet<String> = std::collections::HashSet::new();
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
        .map_err(ApiErrorKind::from)?;

    Ok(())
}
async fn health_check() -> &'static str {
    "ok"
}
