use std::collections::HashSet;
use std::fs;
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

mod gc;

use anyhow::{Context, Result, anyhow};
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
use futures::{StreamExt, TryStreamExt, stream::FuturesUnordered};
use pointer_indexer::models::{
    BranchHead, ChunkMapping, ContentBlob, FilePointer, ReferenceRecord, SymbolNamespaceRecord,
    SymbolRecord, UniqueChunk,
};
use serde::{Deserialize, Serialize, de::IgnoredAny};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Acquire, PgConnection, PgPool, Postgres, QueryBuilder, Transaction};
use tempfile::Builder;
use thiserror::Error;
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader as TokioBufReader};
use tokio::net::TcpListener;
use tokio::{signal, time};
use tracing::info;

use crate::gc::{
    GarbageCollector, is_latest_commit_on_any_branch, prune_commit_data, prune_repository_data,
};
use zstd::stream::read::Decoder;
use chrono::Utc;

#[derive(Debug, Parser)]
struct ServerConfig {
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    #[arg(long, env = "BIND_ADDRESS", default_value = "127.0.0.1:8080")]
    bind: String,
    #[arg(long, env = "MAX_CONNECTIONS", default_value_t = 10)]
    max_connections: u32,
    #[arg(long, env = "SCRATCH_DIR", default_value = ".pointer-backend-scratch")]
    scratch_dir: PathBuf,
    #[arg(long, env = "ENABLE_GC", default_value_t = false)]
    enable_gc: bool,
    #[arg(long, env = "GC_INTERVAL_SECS", default_value_t = 3600)]
    gc_interval_secs: u64,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    scratch_dir: PathBuf,
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
                tracing::error!(error = ?err, "database error");
                AppError::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
            }
            ApiErrorKind::Serde(err) => AppError::new(StatusCode::BAD_REQUEST, err.to_string()),
            ApiErrorKind::Compression(err) => {
                AppError::new(StatusCode::BAD_REQUEST, err.to_string())
            }
            ApiErrorKind::Internal(err) => {
                tracing::error!(error = ?err, "internal error");
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

#[derive(Debug, Deserialize)]
struct ManifestShardPayload {
    section: String,
    shard_index: Option<u64>,
    data: String,
    compressed: Option<bool>,
}

#[derive(sqlx::FromRow)]
struct UploadChunkRow {
    chunk_index: i32,
    total_chunks: i32,
    data: Vec<u8>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "section", content = "payload")]
enum ManifestEnvelope {
    #[serde(rename = "content_blob")]
    ContentBlob(IgnoredAny),
    #[serde(rename = "symbol_namespace")]
    SymbolNamespace(SymbolNamespaceRecord),
    #[serde(rename = "symbol_record")]
    SymbolRecord(SymbolRecord),
    #[serde(rename = "file_pointer")]
    FilePointer(FilePointer),
    #[serde(rename = "reference_record")]
    ReferenceRecord(ReferenceRecord),
    #[serde(rename = "branch_head")]
    BranchHead(BranchHead),
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

    fs::create_dir_all(&config.scratch_dir).with_context(|| {
        format!(
            "failed to create scratch directory {}",
            config.scratch_dir.display()
        )
    })?;

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to postgres")?;

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .context("database migration failed")?;

    let app_state = AppState {
        pool: pool.clone(),
        scratch_dir: config.scratch_dir.clone(),
    };

    if config.enable_gc {
        let interval = Duration::from_secs(config.gc_interval_secs.max(60));
        spawn_gc_loop(pool.clone(), interval);
    }

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
        .route("/api/v1/manifest/shard", post(manifest_shard))
        .route("/api/v1/index/manifest/shard", post(manifest_shard))
        // Manifest upload routes
        .route("/api/v1/manifest/chunk", post(manifest_chunk))
        .route("/api/v1/manifest/finalize", post(manifest_finalize))
        .route("/api/v1/index/manifest/chunk", post(manifest_chunk))
        .route("/api/v1/index/manifest/finalize", post(manifest_finalize))
        // Pruning routes
        .route("/api/v1/prune/commit", post(prune_commit_handler))
        .route("/api/v1/prune/branch", post(prune_branch_handler))
        .route("/api/v1/prune/repo", post(prune_repo_handler))
        .route("/api/v1/prune/policy", post(apply_retention_policy_handler))
        .route("/api/v1/admin/gc", post(run_gc_handler))
        .route("/api/v1/admin/rebuild_symbol_cache", post(rebuild_symbol_cache_handler))
        .route(
            "/api/v1/admin/cleanup_symbol_cache",
            post(cleanup_symbol_cache_handler),
        )
        .route(
            "/api/v1/admin/refresh_symbol_cache",
            post(refresh_symbol_cache_handler),
        )
        .route("/healthz", get(health_check))
        .with_state(app_state)
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

fn spawn_gc_loop(pool: PgPool, interval: Duration) {
    tokio::spawn(async move {
        let collector = GarbageCollector::new(pool);
        loop {
            if let Err(err) = collector.run_once().await {
                tracing::error!(error = ?err, "background garbage collection run failed");
            }
            time::sleep(interval).await;
        }
    });
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

async fn manifest_shard(
    State(state): State<AppState>,
    Json(payload): Json<ManifestShardPayload>,
) -> ApiResult<StatusCode> {
    let compressed = payload.compressed.unwrap_or(true);
    let bytes = BASE64.decode(payload.data.as_bytes()).map_err(|err| {
        AppError::new(
            StatusCode::BAD_REQUEST,
            format!("invalid base64 data: {err}"),
        )
    })?;

    let data = if compressed {
        let mut decoder = Decoder::new(bytes.as_slice()).map_err(ApiErrorKind::Compression)?;
        let mut out = Vec::new();
        decoder
            .read_to_end(&mut out)
            .map_err(ApiErrorKind::Compression)?;
        out
    } else {
        bytes
    };

    process_manifest_section(&state.pool, &payload.section, payload.shard_index, &data).await?;

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

    let mut temp_file = Builder::new()
        .prefix("pointer-backend-upload")
        .tempfile_in(&state.scratch_dir)
        .map_err(ApiErrorKind::Compression)?;
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

    let mut plain_file = Builder::new()
        .prefix("pointer-backend-manifest")
        .tempfile_in(&state.scratch_dir)
        .map_err(ApiErrorKind::Compression)?;
    if compressed {
        let mut decoder = Decoder::new(temp_file).map_err(ApiErrorKind::Compression)?;
        std::io::copy(&mut decoder, &mut plain_file).map_err(ApiErrorKind::Compression)?;
    } else {
        let mut source = temp_file;
        std::io::copy(&mut source, &mut plain_file).map_err(ApiErrorKind::Compression)?;
    }

    plain_file
        .seek(SeekFrom::Start(0))
        .map_err(ApiErrorKind::Compression)?;

    let std_file = plain_file
        .as_file()
        .try_clone()
        .map_err(ApiErrorKind::Compression)?;
    let reader = TokioBufReader::new(TokioFile::from_std(std_file));
    ingest_manifest_stream(&state.pool, reader).await?;

    sqlx::query("DELETE FROM upload_chunks WHERE upload_id = $1")
        .bind(&payload.upload_id)
        .execute(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::CREATED)
}

async fn process_manifest_section(
    pool: &PgPool,
    section: &str,
    shard_index: Option<u64>,
    data: &[u8],
) -> Result<(), ApiErrorKind> {
    match section {
        "file_pointer" => process_file_pointer_data(pool, data).await?,
        "symbol_namespace" => process_symbol_namespace_data(pool, data).await?,
        "symbol_record" => process_symbol_data(pool, data).await?,
        "reference_record" => process_reference_data(pool, data).await?,
        "branch_head" => process_branch_data(pool, data).await?,
        other => {
            return Err(ApiErrorKind::Internal(anyhow!(
                "unknown manifest shard section: {}",
                other
            )));
        }
    }

    if let Some(idx) = shard_index {
        info!(section = section, shard = idx, "manifest shard ingested");
    }

    Ok(())
}

async fn process_file_pointer_data(pool: &PgPool, data: &[u8]) -> Result<(), ApiErrorKind> {
    let chunks = chunk_records(data, |line| {
        serde_json::from_slice::<FilePointer>(line).map_err(ApiErrorKind::Serde)
    })?;
    ingest_chunks(
        pool,
        chunks,
        insert_file_pointers_batch,
        MAX_PARALLEL_INGEST,
    )
    .await
}

async fn process_symbol_data(pool: &PgPool, data: &[u8]) -> Result<(), ApiErrorKind> {
    let chunks = chunk_records(data, |line| {
        serde_json::from_slice::<SymbolRecord>(line).map_err(ApiErrorKind::Serde)
    })?;
    ingest_chunks(
        pool,
        chunks,
        insert_symbol_records_batch,
        MAX_PARALLEL_INGEST,
    )
    .await
}

async fn process_symbol_namespace_data(pool: &PgPool, data: &[u8]) -> Result<(), ApiErrorKind> {
    let raw_chunks = chunk_records(data, |line| {
        serde_json::from_slice::<SymbolNamespaceRecord>(line).map_err(ApiErrorKind::Serde)
    })?;
    let string_chunks: Vec<Vec<String>> = raw_chunks
        .into_iter()
        .map(|chunk| chunk.into_iter().map(|record| record.namespace).collect())
        .collect();
    ingest_chunks(
        pool,
        string_chunks,
        insert_symbol_namespaces_batch,
        MAX_PARALLEL_INGEST,
    )
    .await
}

async fn process_reference_data(pool: &PgPool, data: &[u8]) -> Result<(), ApiErrorKind> {
    let chunks = chunk_records(data, |line| {
        serde_json::from_slice::<ReferenceRecord>(line).map_err(ApiErrorKind::Serde)
    })?;
    ingest_chunks(
        pool,
        chunks,
        insert_reference_records_batch,
        MAX_PARALLEL_INGEST,
    )
    .await
}

async fn process_branch_data(pool: &PgPool, data: &[u8]) -> Result<(), ApiErrorKind> {
    let batches = chunk_records(data, |line| {
        serde_json::from_slice::<BranchHead>(line).map_err(ApiErrorKind::Serde)
    })?;
    ingest_chunks(
        pool,
        batches,
        upsert_branch_heads_batch,
        MAX_PARALLEL_INGEST,
    )
    .await
}

async fn ingest_manifest_stream<R>(pool: &PgPool, reader: R) -> Result<(), ApiErrorKind>
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    let mut file_buffer: Vec<FilePointer> = Vec::with_capacity(INSERT_BATCH_SIZE);
    let mut symbol_buffer: Vec<SymbolRecord> = Vec::with_capacity(INSERT_BATCH_SIZE);
    let mut namespace_buffer: Vec<SymbolNamespaceRecord> = Vec::with_capacity(INSERT_BATCH_SIZE);
    let mut reference_buffer: Vec<ReferenceRecord> = Vec::with_capacity(INSERT_BATCH_SIZE);
    let mut branches: Vec<BranchHead> = Vec::new();

    while let Some(line) = lines.next_line().await.map_err(ApiErrorKind::Compression)? {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let envelope: ManifestEnvelope =
            serde_json::from_str(trimmed).map_err(ApiErrorKind::Serde)?;

        match envelope {
            ManifestEnvelope::ContentBlob(_) => {}
            ManifestEnvelope::SymbolNamespace(namespace) => {
                namespace_buffer.push(namespace);
                if namespace_buffer.len() >= INSERT_BATCH_SIZE {
                    let chunk = mem::take(&mut namespace_buffer)
                        .into_iter()
                        .map(|record| record.namespace)
                        .collect::<Vec<_>>();
                    ingest_chunks(
                        pool,
                        vec![chunk],
                        insert_symbol_namespaces_batch,
                        MAX_PARALLEL_INGEST,
                    )
                    .await?;
                }
            }
            ManifestEnvelope::FilePointer(pointer) => {
                file_buffer.push(pointer);
                if file_buffer.len() >= INSERT_BATCH_SIZE {
                    let chunk = mem::take(&mut file_buffer);
                    ingest_chunks(
                        pool,
                        vec![chunk],
                        insert_file_pointers_batch,
                        MAX_PARALLEL_INGEST,
                    )
                    .await?;
                }
            }
            ManifestEnvelope::SymbolRecord(symbol) => {
                symbol_buffer.push(symbol);
                if symbol_buffer.len() >= INSERT_BATCH_SIZE {
                    let chunk = mem::take(&mut symbol_buffer);
                    ingest_chunks(
                        pool,
                        vec![chunk],
                        insert_symbol_records_batch,
                        MAX_PARALLEL_INGEST,
                    )
                    .await?;
                }
            }
            ManifestEnvelope::ReferenceRecord(reference) => {
                reference_buffer.push(reference);
                if reference_buffer.len() >= INSERT_BATCH_SIZE {
                    let chunk = mem::take(&mut reference_buffer);
                    ingest_chunks(
                        pool,
                        vec![chunk],
                        insert_reference_records_batch,
                        MAX_PARALLEL_INGEST,
                    )
                    .await?;
                }
            }
            ManifestEnvelope::BranchHead(branch) => {
                branches.push(branch);
            }
        }
    }

    if !file_buffer.is_empty() {
        ingest_chunks(
            pool,
            vec![file_buffer],
            insert_file_pointers_batch,
            MAX_PARALLEL_INGEST,
        )
        .await?;
    }
    if !symbol_buffer.is_empty() {
        ingest_chunks(
            pool,
            vec![symbol_buffer],
            insert_symbol_records_batch,
            MAX_PARALLEL_INGEST,
        )
        .await?;
    }
    if !namespace_buffer.is_empty() {
        let chunk = namespace_buffer
            .into_iter()
            .map(|record| record.namespace)
            .collect::<Vec<_>>();
        ingest_chunks(
            pool,
            vec![chunk],
            insert_symbol_namespaces_batch,
            MAX_PARALLEL_INGEST,
        )
        .await?;
    }
    if !reference_buffer.is_empty() {
        ingest_chunks(
            pool,
            vec![reference_buffer],
            insert_reference_records_batch,
            MAX_PARALLEL_INGEST,
        )
        .await?;
    }
    if !branches.is_empty() {
        ingest_chunks(
            pool,
            chunk_vec(branches),
            upsert_branch_heads_batch,
            MAX_PARALLEL_INGEST,
        )
        .await?;
    }

    Ok(())
}

const INSERT_BATCH_SIZE: usize = 1000;
const MAX_PARALLEL_INGEST: usize = 8;

fn chunk_records<T, F>(data: &[u8], mut parse: F) -> Result<Vec<Vec<T>>, ApiErrorKind>
where
    T: Send,
    F: FnMut(&[u8]) -> Result<T, ApiErrorKind>,
{
    let mut chunks = Vec::new();
    let mut buffer = Vec::with_capacity(INSERT_BATCH_SIZE);

    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }

        let record = parse(line)?;
        buffer.push(record);

        if buffer.len() >= INSERT_BATCH_SIZE {
            chunks.push(mem::take(&mut buffer));
            buffer = Vec::with_capacity(INSERT_BATCH_SIZE);
        }
    }

    if !buffer.is_empty() {
        chunks.push(buffer);
    }

    Ok(chunks)
}

fn chunk_vec<T>(records: Vec<T>) -> Vec<Vec<T>> {
    if records.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut current = Vec::with_capacity(INSERT_BATCH_SIZE);

    for record in records {
        current.push(record);
        if current.len() >= INSERT_BATCH_SIZE {
            chunks.push(mem::take(&mut current));
            current = Vec::with_capacity(INSERT_BATCH_SIZE);
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    chunks
}

async fn ingest_chunks<T, Fut>(
    pool: &PgPool,
    chunks: Vec<Vec<T>>,
    make_task: impl Fn(PgPool, Vec<T>) -> Fut + Send + Sync,
    max_parallel: usize,
) -> Result<(), ApiErrorKind>
where
    T: Send + 'static,
    Fut: Future<Output = Result<(), ApiErrorKind>> + Send + 'static,
{
    let mut futures = FuturesUnordered::new();

    for chunk in chunks.into_iter() {
        let pool_clone = pool.clone();
        futures.push(tokio::spawn(make_task(pool_clone, chunk)));

        if futures.len() >= max_parallel && max_parallel > 0 {
            if let Some(res) = futures.next().await {
                res.map_err(|err| ApiErrorKind::Internal(anyhow!(err)))??;
            }
        }
    }

    while let Some(res) = futures.next().await {
        res.map_err(|err| ApiErrorKind::Internal(anyhow!(err)))??;
    }

    Ok(())
}

async fn insert_file_pointers_batch(
    pool: PgPool,
    chunk: Vec<FilePointer>,
) -> Result<(), ApiErrorKind> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut qb =
        QueryBuilder::new("INSERT INTO files (repository, commit_sha, file_path, content_hash) ");
    qb.push_values(chunk.iter(), |mut b, file| {
        b.push_bind(&file.repository)
            .push_bind(&file.commit_sha)
            .push_bind(&file.file_path)
            .push_bind(&file.content_hash);
    });
    qb.push(
        " ON CONFLICT (repository, commit_sha, file_path) DO UPDATE SET content_hash = EXCLUDED.content_hash",
    );

    qb.build()
        .execute(&pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(())
}

async fn insert_symbol_records_batch(
    pool: PgPool,
    chunk: Vec<SymbolRecord>,
) -> Result<(), ApiErrorKind> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut conn = pool.acquire().await.map_err(ApiErrorKind::from)?;

    let mut symbol_qb = QueryBuilder::new("INSERT INTO symbols (content_hash, name, name_lc) ");
    symbol_qb.push_values(chunk.iter(), |mut b, symbol| {
        let name_lc = symbol.name.to_lowercase();
        b.push_bind(&symbol.content_hash)
            .push_bind(&symbol.name)
            .push_bind(name_lc);
    });
    symbol_qb.push(" ON CONFLICT (content_hash, name) DO NOTHING");
    symbol_qb
        .build()
        .execute(&mut *conn)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(())
}

async fn insert_symbol_namespaces_batch(
    pool: PgPool,
    chunk: Vec<String>,
) -> Result<(), ApiErrorKind> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut unique = HashSet::with_capacity(chunk.len());
    let mut values = Vec::new();
    for namespace in chunk {
        if unique.insert(namespace.clone()) {
            values.push(namespace);
        }
    }
    if values.is_empty() {
        return Ok(());
    }

    let mut qb = QueryBuilder::new("INSERT INTO symbol_namespaces (namespace) ");
    qb.push_values(values.iter(), |mut b, namespace| {
        b.push_bind(namespace);
    });
    qb.push(" ON CONFLICT (namespace) DO NOTHING");

    qb.build()
        .execute(&pool)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(())
}

async fn insert_reference_records_batch(
    pool: PgPool,
    chunk: Vec<ReferenceRecord>,
) -> Result<(), ApiErrorKind> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut conn = pool
        .acquire()
        .await
        .map_err(|err| ApiErrorKind::from(err))?;
    let mut tx: Transaction<'_, Postgres> =
        conn.begin().await.map_err(|err| ApiErrorKind::from(err))?;

    sqlx::query(
        "CREATE TEMP TABLE staging_symbol_references (
            content_hash TEXT,
            namespace TEXT,
            name TEXT,
            kind TEXT,
            line_number INT,
            column_number INT
        ) ON COMMIT DROP",
    )
    .execute(&mut *tx)
    .await
    .map_err(|err| ApiErrorKind::from(err))?;

    let mut staging_qb = QueryBuilder::new(
        "INSERT INTO staging_symbol_references (content_hash, namespace, name, kind, line_number, column_number) ",
    );
    staging_qb.push_values(chunk.iter(), |mut b, reference| {
        let line: i32 = reference.line.try_into().unwrap_or(i32::MAX);
        let column: i32 = reference.column.try_into().unwrap_or(i32::MAX);
        let namespace = reference.namespace.as_deref().unwrap_or("");
        b.push_bind(&reference.content_hash)
            .push_bind(namespace)
            .push_bind(&reference.name)
            .push_bind(&reference.kind)
            .push_bind(line)
            .push_bind(column);
    });
    staging_qb
        .build()
        .execute(&mut *tx)
        .await
        .map_err(|err| ApiErrorKind::from(err))?;

    sqlx::query(
        "INSERT INTO symbol_references (symbol_id, namespace_id, kind, line_number, column_number)
         SELECT s.id, sn.id, data.kind, data.line_number, data.column_number
         FROM (
             SELECT content_hash, namespace, name, kind, line_number, column_number
             FROM staging_symbol_references
             ORDER BY namespace, content_hash, name, line_number, column_number, kind
         ) AS data
         JOIN symbols s
           ON s.content_hash = data.content_hash
          AND s.name = data.name
         JOIN symbol_namespaces sn
           ON sn.namespace = data.namespace
         ON CONFLICT (symbol_id, namespace_id, line_number, column_number, kind) DO NOTHING",
    )
    .execute(&mut *tx)
    .await
    .map_err(|err| ApiErrorKind::from(err))?;

    tx.commit().await.map_err(|err| ApiErrorKind::from(err))?;

    Ok(())
}

async fn upsert_branch_heads_batch(
    pool: PgPool,
    chunk: Vec<BranchHead>,
) -> Result<(), ApiErrorKind> {
    if chunk.is_empty() {
        return Ok(());
    }

    let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;

    let mut qb = QueryBuilder::new("INSERT INTO branches (repository, branch, commit_sha) ");
    qb.push_values(chunk.iter(), |mut b, branch| {
        b.push_bind(&branch.repository)
            .push_bind(&branch.branch)
            .push_bind(&branch.commit_sha);
    });
    qb.push(
        " ON CONFLICT (repository, branch)
          DO UPDATE SET commit_sha = EXCLUDED.commit_sha, indexed_at = NOW()",
    );

    qb.build()
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

    for branch in &chunk {
        let (policy_latest_keep, snapshot_specs, policy_specified, live_state) =
            if let Some(policy) = &branch.policy {
                (
                    policy.latest_keep_count,
                    policy.snapshot_policies.as_slice(),
                    true,
                    policy.is_live,
                )
            } else {
                (1, &[][..], false, None)
            };
        if policy_latest_keep == 0 {
            return Err(ApiErrorKind::Internal(anyhow!(
                "latest_keep_count must be positive for branch {}",
                branch.branch
            )));
        }
        let latest_keep = i32::try_from(policy_latest_keep).map_err(|_| {
            ApiErrorKind::Internal(anyhow!(
                "latest_keep_count exceeds supported range for branch {}: {}",
                branch.branch,
                policy_latest_keep
            ))
        })?;

        sqlx::query(
            "INSERT INTO branch_policies (repository, branch, latest_keep_count, updated_at)
                 VALUES ($1, $2, $3, NOW())
                 ON CONFLICT (repository, branch)
                 DO UPDATE SET latest_keep_count = EXCLUDED.latest_keep_count,
                               updated_at = NOW()",
        )
        .bind(&branch.repository)
        .bind(&branch.branch)
        .bind(latest_keep)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

        if policy_specified {
            sqlx::query(
                "DELETE FROM branch_snapshot_policies WHERE repository = $1 AND branch = $2",
            )
            .bind(&branch.repository)
            .bind(&branch.branch)
            .execute(&mut *tx)
            .await
            .map_err(ApiErrorKind::from)?;

            if !snapshot_specs.is_empty() {
                let mut seen_intervals = HashSet::new();
                let mut sanitized = Vec::new();
                for snap in snapshot_specs {
                    let interval_seconds = i64::try_from(snap.interval_seconds).map_err(|_| {
                        ApiErrorKind::Internal(anyhow!(
                            "snapshot policy interval exceeds supported range for branch {}",
                            branch.branch
                        ))
                    })?;
                    if interval_seconds <= 0 {
                        return Err(ApiErrorKind::Internal(anyhow!(
                            "snapshot policy interval must be positive for branch {}",
                            branch.branch
                        )));
                    }
                    let keep_count = i32::try_from(snap.keep_count).map_err(|_| {
                        ApiErrorKind::Internal(anyhow!(
                            "snapshot policy count exceeds supported range for branch {}",
                            branch.branch
                        ))
                    })?;
                    if keep_count <= 0 {
                        return Err(ApiErrorKind::Internal(anyhow!(
                            "snapshot policy count must be positive for branch {}",
                            branch.branch
                        )));
                    }
                    if seen_intervals.insert(interval_seconds) {
                        sanitized.push((interval_seconds, keep_count));
                    }
                }

                if !sanitized.is_empty() {
                    let mut snapshot_policy_qb = QueryBuilder::new(
                        "INSERT INTO branch_snapshot_policies (repository, branch, interval_seconds, keep_count) ",
                    );
                    snapshot_policy_qb.push_values(sanitized.iter(), |mut b, (interval, count)| {
                        b.push_bind(&branch.repository)
                            .push_bind(&branch.branch)
                            .push_bind(interval)
                            .push_bind(count);
                    });
                    snapshot_policy_qb.push(
                        " ON CONFLICT (repository, branch, interval_seconds)
                      DO UPDATE SET keep_count = EXCLUDED.keep_count, created_at = NOW()",
                    );
                    snapshot_policy_qb
                        .build()
                        .execute(&mut *tx)
                        .await
                        .map_err(ApiErrorKind::from)?;
                }
            }

            match live_state {
                Some(true) => {
                    sqlx::query(
                        "INSERT INTO repo_live_branches (repository, branch, updated_at)
                         VALUES ($1, $2, NOW())
                         ON CONFLICT (repository)
                         DO UPDATE SET branch = EXCLUDED.branch, updated_at = NOW()",
                    )
                    .bind(&branch.repository)
                    .bind(&branch.branch)
                    .execute(&mut *tx)
                    .await
                    .map_err(ApiErrorKind::from)?;
                }
                Some(false) => {
                    sqlx::query(
                        "DELETE FROM repo_live_branches WHERE repository = $1 AND branch = $2",
                    )
                    .bind(&branch.repository)
                    .bind(&branch.branch)
                    .execute(&mut *tx)
                    .await
                    .map_err(ApiErrorKind::from)?;
                }
                None => {}
            }
        }
    }

    let mut snapshot_qb =
        QueryBuilder::new("INSERT INTO branch_snapshots (repository, branch, commit_sha) ");
    snapshot_qb.push_values(chunk.iter(), |mut b, branch| {
        b.push_bind(&branch.repository)
            .push_bind(&branch.branch)
            .push_bind(&branch.commit_sha);
    });
    snapshot_qb.push(" ON CONFLICT DO NOTHING");
    snapshot_qb
        .build()
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;

    tx.commit().await.map_err(ApiErrorKind::from)?;

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
struct PruneRepoRequest {
    repository: String,
    #[serde(default = "default_prune_repo_batch_size")]
    batch_size: i64,
}

#[derive(Debug, Serialize)]
struct PruneRepoResponse {
    repository: String,
    pruned: bool,
    deleted_rows: i64,
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

#[derive(Debug, Serialize)]
struct GcResponse {
    branches_evaluated: usize,
    snapshots_removed: usize,
    commits_pruned: usize,
}

#[derive(Debug, Deserialize)]
struct CleanupSymbolCacheRequest {
    #[serde(default = "default_symbol_cache_batch_size")]
    batch_size: i64,
    #[serde(default = "default_symbol_cache_max_batches")]
    max_batches: i64,
}

#[derive(Debug, Serialize)]
struct CleanupSymbolCacheResponse {
    refs_deleted: i64,
    names_deleted: i64,
    batches_run: i64,
}

#[derive(Debug, Deserialize)]
struct RefreshSymbolCacheRequest {
    #[serde(default = "default_symbol_cache_batch_size")]
    batch_size: i64,
    #[serde(default = "default_symbol_cache_max_batches")]
    max_batches: i64,
}

#[derive(Debug, Serialize)]
struct RefreshSymbolCacheResponse {
    names_inserted: i64,
    batches_run: i64,
}

#[derive(Debug, Serialize)]
struct RebuildSymbolCacheResponse {
    message: String,
    shard_count: usize,
    inserted_names: u64,
    inserted_refs: u64,
}

// Manual prune for a specific commit
async fn prune_commit_handler(
    State(state): State<AppState>,
    Json(payload): Json<PruneCommitRequest>,
) -> ApiResult<Json<PruneCommitResponse>> {
    let is_latest =
        is_latest_commit_on_any_branch(&state.pool, &payload.repository, &payload.commit_sha)
            .await?;

    if is_latest {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "Cannot prune commit that is the latest on a branch. Update the branch first.",
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

// Prune all commits for a specific branch except the latest
async fn prune_branch_handler(
    State(state): State<AppState>,
    Json(payload): Json<PruneBranchRequest>,
) -> ApiResult<Json<PruneBranchResponse>> {
    // Get the latest commit for this branch
    let latest_commit_opt: Option<(String,)> =
        sqlx::query_as("SELECT commit_sha FROM branches WHERE repository = $1 AND branch = $2")
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
        "SELECT DISTINCT commit_sha FROM files WHERE repository = $1 AND commit_sha != $2",
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
        message: format!(
            "Pruned {} commits from branch (kept latest commit {})",
            pruned_count, latest_commit
        ),
    }))
}

async fn prune_repo_handler(
    State(state): State<AppState>,
    Json(payload): Json<PruneRepoRequest>,
) -> ApiResult<Json<PruneRepoResponse>> {
    let deleted_rows =
        prune_repository_data(&state.pool, &payload.repository, payload.batch_size).await?;
    let pruned = deleted_rows > 0;

    Ok(Json(PruneRepoResponse {
        repository: payload.repository,
        pruned,
        deleted_rows,
        message: if pruned {
            "Repository data successfully pruned".to_string()
        } else {
            "No data found for the specified repository".to_string()
        },
    }))
}

async fn run_gc_handler(State(state): State<AppState>) -> ApiResult<Json<GcResponse>> {
    let collector = GarbageCollector::new(state.pool.clone());
    let outcome = collector.run_once().await?;
    Ok(Json(GcResponse {
        branches_evaluated: outcome.branches_evaluated,
        snapshots_removed: outcome.snapshots_removed,
        commits_pruned: outcome.commits_pruned,
    }))
}

async fn cleanup_symbol_cache_handler(
    State(state): State<AppState>,
    Json(payload): Json<CleanupSymbolCacheRequest>,
) -> ApiResult<Json<CleanupSymbolCacheResponse>> {
    let batch_size = payload.batch_size.max(1);
    let max_batches = payload.max_batches.max(1);
    let mut names_deleted = 0_i64;
    let mut batches_run = 0_i64;

    let mut conn = state.pool.acquire().await.map_err(ApiErrorKind::from)?;

    for _ in 0..max_batches {
        let result = sqlx::query(
            "
            WITH doomed AS (
                SELECT us.name_lc
                FROM unique_symbols us
                LEFT JOIN symbols s ON s.name_lc = us.name_lc
                WHERE s.name_lc IS NULL
                LIMIT $1
            )
            DELETE FROM unique_symbols us
            USING doomed
            WHERE us.name_lc = doomed.name_lc
            ",
        )
        .bind(batch_size)
        .execute(&mut *conn)
        .await
        .map_err(ApiErrorKind::from)?;

        let deleted = result.rows_affected() as i64;
        names_deleted = names_deleted.saturating_add(deleted);
        batches_run = batches_run.saturating_add(1);
        if deleted == 0 {
            break;
        }
    }

    Ok(Json(CleanupSymbolCacheResponse {
        refs_deleted: 0,
        names_deleted,
        batches_run,
    }))
}

async fn refresh_symbol_cache_handler(
    State(state): State<AppState>,
    Json(payload): Json<RefreshSymbolCacheRequest>,
) -> ApiResult<Json<RefreshSymbolCacheResponse>> {
    let batch_size = payload.batch_size.max(1);
    let max_batches = payload.max_batches.max(1);
    let mut names_inserted = 0_i64;
    let mut batches_run = 0_i64;

    let mut conn = state.pool.acquire().await.map_err(ApiErrorKind::from)?;

    for _ in 0..max_batches {
        let result = sqlx::query(
            "
            WITH missing AS (
                SELECT s.name_lc, MIN(s.name) AS name
                FROM symbols s
                LEFT JOIN unique_symbols us ON us.name_lc = s.name_lc
                WHERE us.name_lc IS NULL
                GROUP BY s.name_lc
                LIMIT $1
            )
            INSERT INTO unique_symbols (name_lc, name)
            SELECT missing.name_lc, missing.name
            FROM missing
            ON CONFLICT (name_lc) DO NOTHING
            ",
        )
        .bind(batch_size)
        .execute(&mut *conn)
        .await
        .map_err(ApiErrorKind::from)?;

        let inserted = result.rows_affected() as i64;
        names_inserted = names_inserted.saturating_add(inserted);
        batches_run = batches_run.saturating_add(1);
        if inserted == 0 {
            break;
        }
    }

    Ok(Json(RefreshSymbolCacheResponse {
        names_inserted,
        batches_run,
    }))
}

async fn rebuild_symbol_cache_handler(
    State(state): State<AppState>,
) -> ApiResult<Json<RebuildSymbolCacheResponse>> {
    const MAX_SYMBOL_CACHE_WORKERS: usize = 8;
    let shard_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .min(MAX_SYMBOL_CACHE_WORKERS)
        .max(1);

    let mut lock_conn = state.pool.acquire().await.map_err(ApiErrorKind::from)?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(983_475_023_i64)
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;

    sqlx::query("CREATE TABLE IF NOT EXISTS unique_symbols_new (LIKE unique_symbols INCLUDING ALL)")
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;
    sqlx::query("TRUNCATE unique_symbols_new")
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;

    let mut tasks = FuturesUnordered::new();
    for shard in 0..shard_count {
        let pool = state.pool.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = pool.acquire().await?;
            let names_result = sqlx::query(
                "
                INSERT INTO unique_symbols_new (name_lc, name)
                SELECT
                    name_lc,
                    MIN(name) AS name
                FROM (
                    SELECT
                        name,
                        name_lc
                    FROM symbols
                    WHERE MOD(ABS(hashtext(name_lc)), $1) = $2
                ) t
                GROUP BY name_lc
                ",
            )
            .bind(shard_count as i64)
            .bind(shard as i64)
            .execute(&mut *conn)
            .await?;

            Ok::<_, sqlx::Error>(names_result.rows_affected())
        }));
    }

    let mut inserted_names = 0_u64;
    while let Some(result) = tasks.try_next().await.map_err(|err| {
        AppError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("symbol cache rebuild task join failed: {}", err),
        )
    })? {
        let names = result.map_err(ApiErrorKind::from)?;
        inserted_names = inserted_names.saturating_add(names);
    }

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS unique_symbols_new_name_lc_trgm ON unique_symbols_new USING gin (name_lc gin_trgm_ops)",
    )
    .execute(&mut *lock_conn)
    .await
    .map_err(ApiErrorKind::from)?;
    sqlx::query("ANALYZE unique_symbols_new")
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;

    let suffix = Utc::now().format("%Y%m%d%H%M%S").to_string();
    rename_table_if_exists(
        &mut *lock_conn,
        "unique_symbols_old",
        &format!("unique_symbols_old_{}", suffix),
    )
    .await?;

    sqlx::query("ALTER TABLE unique_symbols RENAME TO unique_symbols_old")
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;
    sqlx::query("ALTER TABLE unique_symbols_new RENAME TO unique_symbols")
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;

    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(983_475_023_i64)
        .execute(&mut *lock_conn)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(Json(RebuildSymbolCacheResponse {
        message: "rebuilt symbol cache".to_string(),
        shard_count,
        inserted_names,
        inserted_refs: 0,
    }))
}

async fn rename_table_if_exists(
    conn: &mut PgConnection,
    from: &str,
    to: &str,
) -> std::result::Result<(), ApiErrorKind> {
    let full_name = format!("public.{}", from);
    let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)")
        .bind(full_name)
        .fetch_one(&mut *conn)
        .await
        .map_err(ApiErrorKind::from)?;
    if exists.is_some() {
        let sql = format!("ALTER TABLE {} RENAME TO {}", from, to);
        sqlx::query(&sql)
            .execute(&mut *conn)
            .await
            .map_err(ApiErrorKind::from)?;
    }
    Ok(())
}

fn default_symbol_cache_batch_size() -> i64 {
    10_000
}

fn default_symbol_cache_max_batches() -> i64 {
    50
}

fn default_prune_repo_batch_size() -> i64 {
    10_000
}

// Retention Policy Structures
#[derive(Debug, Deserialize)]
struct RetentionPolicyConfig {
    repository: String,
    keep_latest: bool,

    max_commits_to_keep: Option<i32>,
}

#[derive(Debug, Serialize)]
struct RetentionPolicyResponse {
    repository: String,
    message: String,
}

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
async fn apply_retention_policy(
    pool: &PgPool,
    config: &RetentionPolicyConfig,
) -> std::result::Result<(), ApiErrorKind> {
    // Get all commits for this repository from the files table
    let all_commits: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT commit_sha FROM files WHERE repository = $1")
            .bind(&config.repository)
            .fetch_all(pool)
            .await
            .map_err(ApiErrorKind::from)?;

    let mut commits_to_keep = HashSet::new();

    // Always keep the latest commit on each branch
    if config.keep_latest {
        let latest_branch_commits: Vec<(String,)> =
            sqlx::query_as("SELECT commit_sha FROM branches WHERE repository = $1")
                .bind(&config.repository)
                .fetch_all(pool)
                .await
                .map_err(ApiErrorKind::from)?;

        for (commit_sha,) in latest_branch_commits {
            commits_to_keep.insert(commit_sha);
        }
    }

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
             LIMIT $2",
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

async fn health_check() -> &'static str {
    "ok"
}
