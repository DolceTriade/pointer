use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use pointer_indexer::models::{
    ContentBlob, FilePointer, IndexReport, ReferenceRecord, SymbolRecord,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, QueryBuilder};
use thiserror::Error;
use tokio::signal;
use tower_http::trace::TraceLayer;
use tracing::{error, info};
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
        .route("/api/v1/search", post(search_symbols))
        .route("/healthz", get(health_check))
        .with_state(state)
        .layer(TraceLayer::new_for_http());

    info!(%bind_addr, "server starting");

    axum::Server::bind(&bind_addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server shutdown")?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install TERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received");
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

    for ContentBlob {
        hash,
        language,
        byte_len,
        line_count,
    } in report.content_blobs
    {
        sqlx::query(
            "INSERT INTO content_blobs (hash, language, byte_len, line_count)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (hash) DO UPDATE SET language = EXCLUDED.language, byte_len = EXCLUDED.byte_len, line_count = EXCLUDED.line_count",
        )
        .bind(&hash)
        .bind(&language)
        .bind(byte_len)
        .bind(line_count)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;
    }

    for FilePointer {
        repository,
        commit_sha,
        file_path,
        content_hash,
    } in report.file_pointers
    {
        sqlx::query(
            "INSERT INTO files (repository, commit_sha, file_path, content_hash)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (repository, commit_sha, file_path) DO UPDATE SET content_hash = EXCLUDED.content_hash",
        )
        .bind(&repository)
        .bind(&commit_sha)
        .bind(&file_path)
        .bind(&content_hash)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;
    }

    for SymbolRecord {
        content_hash,
        namespace,
        symbol,
        fully_qualified,
        kind,
    } in report.symbol_records
    {
        sqlx::query(
            "INSERT INTO symbols (content_hash, namespace, symbol, fully_qualified, kind)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (content_hash, namespace, symbol, kind) DO UPDATE SET fully_qualified = EXCLUDED.fully_qualified",
        )
        .bind(&content_hash)
        .bind(&namespace)
        .bind(&symbol)
        .bind(&fully_qualified)
        .bind(&kind)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;
    }

    for ReferenceRecord {
        content_hash,
        namespace,
        name,
        fully_qualified,
        kind,
        line,
        column,
    } in report.reference_records
    {
        let line: i32 = line.try_into().unwrap_or(i32::MAX);
        let column: i32 = column.try_into().unwrap_or(i32::MAX);
        sqlx::query(
            "INSERT INTO references (content_hash, namespace, name, fully_qualified, kind, line, column)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (content_hash, namespace, name, line, column, kind)
             DO NOTHING",
        )
        .bind(&content_hash)
        .bind(&namespace)
        .bind(&name)
        .bind(&fully_qualified)
        .bind(&kind)
        .bind(line)
        .bind(column)
        .execute(&mut *tx)
        .await
        .map_err(ApiErrorKind::from)?;
    }

    tx.commit().await.map_err(ApiErrorKind::from)?;

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
    content_hash: String,
}

#[derive(sqlx::FromRow)]
struct ReferenceRow {
    content_hash: String,
    name: String,
    namespace: Option<String>,
    kind: Option<String>,
    fully_qualified: String,
    line: i32,
    column: i32,
}

async fn search_symbols(
    State(state): State<AppState>,
    Json(request): Json<SearchRequest>,
) -> ApiResult<Json<SearchResponse>> {
    let mut qb = QueryBuilder::new(
        "SELECT s.symbol, s.namespace, s.kind, s.fully_qualified, cb.language, \
         f.repository, f.commit_sha, f.file_path, s.content_hash \
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
        let hashes: Vec<String> = rows.iter().map(|row| row.content_hash.clone()).collect();
        if !hashes.is_empty() {
            let ref_rows: Vec<ReferenceRow> = sqlx::query_as(
                "SELECT content_hash, name, namespace, kind, fully_qualified, line, column \
                 FROM references WHERE content_hash = ANY($1)",
            )
            .bind(&hashes)
            .fetch_all(&state.pool)
            .await
            .map_err(ApiErrorKind::from)?;

            for reference in ref_rows {
                reference_map
                    .entry(reference.content_hash)
                    .or_insert_with(Vec::new)
                    .push(reference);
            }
        }
    }

    let mut results = Vec::new();

    for row in rows {
        let references = if include_refs {
            reference_map.remove(&row.content_hash).map(|refs| {
                refs.into_iter()
                    .map(|r| ReferenceResult {
                        name: r.name,
                        namespace: r.namespace,
                        kind: r.kind,
                        fully_qualified: r.fully_qualified,
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
