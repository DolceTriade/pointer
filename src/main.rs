#[cfg(feature = "ssr")]
pub mod server {
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
}

#[cfg(feature = "ssr")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::sync::Arc;

    use clap::Parser;
    dotenvy::dotenv().ok();
    let config = crate::server::ServerConfig::parse();

    use tokio::sync::Mutex;
    // Initialize logging system with colored output
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::{fmt, EnvFilter};

    let fmt_layer = fmt::layer()
        .with_ansi(true) // Enable colored output
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(false)
        .with_thread_names(true);

    let filter_layer = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    use anyhow::bail;
    use anyhow::Context;
    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use pointer::app::*;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to postgres")?;

    let state = Arc::new(Mutex::new(crate::server::AppState{ pool }));

    tracing::info!("Starting pointer web UI");

    let conf = get_configuration(Some("Cargo.toml")).expect("Failed to read configuration");
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    let shell_options = leptos_options.clone();
    // Generate the list of routes in your Leptos App
    let routes = generate_route_list(App);

    let app = Router::new()
        .leptos_routes(&leptos_options, routes, {
            let state = state.clone();
            move || {
            let val = shell_options.clone();
            provide_context(state.clone());
            move || shell(val.clone())
        }})
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options);

    tracing::info!("listening on http://{}", &addr);

    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                eprintln!("Server error: {}", e);
                bail!("server error");
            }
        }
        Err(e) => {
            eprintln!("Failed to bind to address {}: {}", addr, e);
            bail!("failed to bind");
        }
    }
    return Ok(());
}

#[cfg(not(feature = "ssr"))]
pub fn main() {
    // no client-side main function
    // unless we want this to work with e.g., Trunk for pure client-side testing
    // see lib.rs for hydration function instead
}
