use std::sync::Arc;

use clap::Parser;
use sqlx::postgres::PgPool;
use tokio::sync::Mutex;

#[derive(Debug, Parser)]
pub struct ServerConfig {
    /// Postgres connection string
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,
    /// Address to bind the HTTP server to
    #[arg(long, env = "BIND_ADDRESS", default_value = "127.0.0.1:8080")]
    pub bind: String,
    /// Maximum database connections
    #[arg(long, env = "MAX_CONNECTIONS", default_value_t = 10)]
    pub max_connections: u32,
}

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

pub type GlobalAppState = Arc<Mutex<AppState>>;
