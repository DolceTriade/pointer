use std::collections::{HashMap, HashSet};
use std::io::{Cursor, Read};
use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use clap::Parser;
use pointer_indexer::models::{
    ChunkDescriptor, ContentBlob, FileChunkRecord, FilePointer, IndexReport, ReferenceRecord,
    SymbolRecord,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres, QueryBuilder, Transaction};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{info, warn};
use zstd::stream::read::Decoder;

#[derive(Debug, Parser)]
struct ServerConfig {
    /// Postgres connection string
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
    /// Address to bind the HTTP server to
    #[arg(long, env = "BIND_ADDRESS", default_value = "127.0.0.1:8080")]
    bind: String,
    /// Maximum database connections
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

#[derive(Debug, Deserialize)]
struct ChunkNeedRequest {
    chunks: Vec<ChunkDescriptor>,
}

#[derive(Debug, Serialize)]
struct ChunkNeedResponse {
    missing: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ChunkUploadRequest {
    chunks: Vec<ChunkUploadItem>,
}

#[derive(Debug, Deserialize)]
struct ChunkUploadItem {
    hash: String,
    algorithm: String,
    byte_len: u32,
    data: String,
}

#[derive(Debug, Deserialize)]
struct ManifestRequest {
    report: IndexReport,
}

struct DecodedChunk {
    hash: String,
    algorithm: String,
    byte_len: u32,
    data: Vec<u8>,
}

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

#[derive(Debug, Deserialize)]
struct SnippetRequest {
    repository: String,
    commit_sha: String,
    file_path: String,
    line: u32,
    context: Option<u32>,
}

#[derive(Debug, Serialize)]
struct SnippetResponse {
    start_line: u32,
    highlight_line: u32,
    total_lines: u32,
    lines: Vec<String>,
    truncated: bool,
}

#[derive(sqlx::FromRow)]
struct FileChunkDataRow {
    chunk_hash: String,
    byte_len: i32,
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
        .route("/api/v1/index", post(ingest_index))
        .route("/api/v1/index/chunks/need", post(chunk_need))
        .route("/api/v1/index/chunks/upload", post(chunk_upload))
        .route("/api/v1/index/manifest/chunk", post(manifest_chunk))
        .route("/api/v1/index/manifest/finalize", post(manifest_finalize))
        .route("/api/v1/index/manifest", post(ingest_manifest))
        .route("/api/v1/files/snippet", post(file_snippet))
        .route("/api/v1/search", post(search_symbols))
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

async fn chunk_need(
    State(state): State<AppState>,
    Json(payload): Json<ChunkNeedRequest>,
) -> ApiResult<Json<ChunkNeedResponse>> {
    let requested: HashSet<String> = payload.chunks.into_iter().map(|chunk| chunk.hash).collect();

    if requested.is_empty() {
        return Ok(Json(ChunkNeedResponse {
            missing: Vec::new(),
        }));
    }

    let hashes: Vec<String> = requested.iter().cloned().collect();
    let existing: Vec<(String,)> = sqlx::query_as("SELECT hash FROM chunks WHERE hash = ANY($1)")
        .bind(&hashes)
        .fetch_all(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    let present: HashSet<String> = existing.into_iter().map(|row| row.0).collect();
    let missing: Vec<String> = requested.difference(&present).cloned().collect();

    Ok(Json(ChunkNeedResponse { missing }))
}

async fn chunk_upload(
    State(state): State<AppState>,
    Json(payload): Json<ChunkUploadRequest>,
) -> ApiResult<StatusCode> {
    if payload.chunks.is_empty() {
        return Ok(StatusCode::ACCEPTED);
    }

    let mut decoded = Vec::with_capacity(payload.chunks.len());
    for chunk in payload.chunks {
        let data = BASE64.decode(chunk.data.as_bytes()).map_err(|err| {
            AppError::new(
                StatusCode::BAD_REQUEST,
                format!("invalid base64 data: {err}"),
            )
        })?;

        if chunk.byte_len != data.len() as u32 {
            warn!(
                hash = %chunk.hash,
                expected = chunk.byte_len,
                actual = data.len(),
                "chunk length mismatch; using decoded length",
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
            .execute(&state.pool)
            .await
            .map_err(ApiErrorKind::from)?;
    }

    Ok(StatusCode::ACCEPTED)
}

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
        "INSERT INTO upload_chunks (upload_id, chunk_index, total_chunks, data)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (upload_id, chunk_index) DO UPDATE
         SET total_chunks = EXCLUDED.total_chunks, data = EXCLUDED.data",
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

async fn file_snippet(
    State(state): State<AppState>,
    Json(request): Json<SnippetRequest>,
) -> ApiResult<Json<SnippetResponse>> {
    if request.line == 0 {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "line numbers are 1-based",
        ));
    }

    let chunk_rows: Vec<FileChunkDataRow> = sqlx::query_as(
        "SELECT chunk_hash, byte_len
         FROM file_chunks
         WHERE repository = $1 AND commit_sha = $2 AND file_path = $3
         ORDER BY chunk_order",
    )
    .bind(&request.repository)
    .bind(&request.commit_sha)
    .bind(&request.file_path)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiErrorKind::from)?;

    if chunk_rows.is_empty() {
        return Err(AppError::new(StatusCode::NOT_FOUND, "file not found"));
    }

    let hashes: Vec<String> = chunk_rows
        .iter()
        .map(|row| row.chunk_hash.clone())
        .collect();
    let data_rows: Vec<(String, Vec<u8>)> =
        sqlx::query_as("SELECT hash, data FROM chunks WHERE hash = ANY($1)")
            .bind(&hashes)
            .fetch_all(&state.pool)
            .await
            .map_err(ApiErrorKind::from)?;

    let chunk_map: HashMap<String, Vec<u8>> = data_rows.into_iter().collect();

    let capacity: usize = chunk_rows
        .iter()
        .map(|row| row.byte_len.max(0) as usize)
        .sum();
    let mut file_bytes = Vec::with_capacity(capacity);

    for row in &chunk_rows {
        let data = chunk_map.get(&row.chunk_hash).ok_or_else(|| {
            AppError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("missing chunk data for {}", row.chunk_hash),
            )
        })?;
        file_bytes.extend_from_slice(data);
    }

    let file_text = String::from_utf8_lossy(&file_bytes);
    let lines: Vec<String> = file_text.lines().map(|line| line.to_string()).collect();

    if lines.is_empty() {
        return Err(AppError::new(StatusCode::BAD_REQUEST, "file is empty"));
    }

    let total_lines = lines.len() as u32;
    if request.line > total_lines {
        return Err(AppError::new(
            StatusCode::BAD_REQUEST,
            "line number exceeds file length",
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

    Ok(Json(SnippetResponse {
        start_line,
        highlight_line: request.line,
        total_lines,
        lines: snippet_lines,
        truncated,
    }))
}

async fn ingest_manifest(
    State(state): State<AppState>,
    Json(payload): Json<ManifestRequest>,
) -> ApiResult<StatusCode> {
    ingest_report(&state.pool, payload.report).await?;
    Ok(StatusCode::CREATED)
}

async fn ingest_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> ApiResult<StatusCode> {
    let encoding = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_ascii_lowercase());

    let report: IndexReport = if matches!(encoding.as_deref(), Some("zstd")) {
        let cursor = Cursor::new(body);
        let mut decoder = Decoder::new(cursor).map_err(ApiErrorKind::Compression)?;
        serde_json::from_reader(&mut decoder).map_err(ApiErrorKind::Serde)?
    } else {
        serde_json::from_slice(&body).map_err(ApiErrorKind::Serde)?
    };

    ingest_report(&state.pool, report)
        .await
        .map_err(ApiErrorKind::from)?;

    Ok(StatusCode::CREATED)
}

async fn ingest_report(pool: &PgPool, report: IndexReport) -> Result<(), ApiErrorKind> {
    let mut tx = pool.begin().await.map_err(ApiErrorKind::from)?;

    insert_content_blobs(&mut tx, &report.content_blobs).await?;
    insert_file_pointers(&mut tx, &report.file_pointers).await?;
    insert_symbol_records(&mut tx, &report.symbol_records).await?;
    insert_reference_records(&mut tx, &report.reference_records).await?;
    insert_file_chunk_records(&mut tx, &report.file_chunks).await?;

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

async fn insert_content_blobs(
    tx: &mut Transaction<'_, Postgres>,
    blobs: &[ContentBlob],
) -> Result<(), ApiErrorKind> {
    if blobs.is_empty() {
        return Ok(());
    }

    let deduped = dedup_by_key(blobs, |blob| blob.hash.clone());

    for chunk in deduped.chunks(INSERT_BATCH_SIZE) {
        let mut qb =
            QueryBuilder::new("INSERT INTO content_blobs (hash, language, byte_len, line_count) ");
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
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
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
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
}

async fn insert_file_chunk_records(
    tx: &mut Transaction<'_, Postgres>,
    records: &[FileChunkRecord],
) -> Result<(), ApiErrorKind> {
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
            .map_err(ApiErrorKind::from)?;
    }

    Ok(())
}

#[derive(Debug, Deserialize)]
struct SearchRequest {
    name: Option<String>,
    name_regex: Option<String>,
    namespace: Option<String>,
    namespace_prefix: Option<String>,
    kind: Option<Vec<String>>,
    language: Option<Vec<String>>,
    repository: Option<String>,
    commit_sha: Option<String>,
    path: Option<String>,
    path_regex: Option<String>,
    include_references: Option<bool>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SearchResponse {
    symbols: Vec<SymbolResult>,
}

#[derive(Debug, Serialize)]
struct SymbolResult {
    symbol: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    repository: String,
    commit_sha: String,
    file_path: String,
    language: Option<String>,
    references: Option<Vec<ReferenceResult>>,
}

#[derive(Debug, Serialize)]
struct ReferenceResult {
    name: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    line: usize,
    column: usize,
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

#[derive(sqlx::FromRow, Clone)]
struct ReferenceRow {
    fully_qualified: String,
    name: String,
    namespace: Option<String>,
    kind: Option<String>,
    line: i32,
    column: i32,
}

async fn search_symbols(
    State(state): State<AppState>,
    Json(request): Json<SearchRequest>,
) -> ApiResult<Json<SearchResponse>> {
    let mut qb = QueryBuilder::new(
        "SELECT s.symbol, s.namespace, s.kind, s.fully_qualified, cb.language, \
         f.repository, f.commit_sha, f.file_path \
         FROM symbols s \
         JOIN content_blobs cb ON cb.hash = s.content_hash \
         JOIN files f ON f.content_hash = s.content_hash \
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
        .fetch_all(&state.pool)
        .await
        .map_err(ApiErrorKind::from)?;

    let include_refs = request.include_references.unwrap_or(false);
    let mut reference_map: HashMap<String, Vec<ReferenceRow>> = HashMap::new();

    if include_refs {
        let fully_qualified: HashSet<String> =
            rows.iter().map(|row| row.fully_qualified.clone()).collect();
        if !fully_qualified.is_empty() {
            let lookup: Vec<String> = fully_qualified.into_iter().collect();
            let ref_rows: Vec<ReferenceRow> = sqlx::query_as(
                "SELECT fully_qualified, name, namespace, kind, line_number AS line, column_number AS column \
                 FROM symbol_references WHERE fully_qualified = ANY($1)",
            )
            .bind(&lookup)
            .fetch_all(&state.pool)
            .await
            .map_err(ApiErrorKind::from)?;

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

    Ok(Json(SearchResponse { symbols: results }))
}

async fn health_check() -> &'static str {
    "ok"
}
