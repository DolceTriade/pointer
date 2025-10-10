#[cfg(feature = "ssr")]
pub mod server;

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
    use tracing_subscriber::{EnvFilter, fmt};

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

    use anyhow::Context;
    use anyhow::bail;
    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{LeptosRoutes, generate_route_list_with_exclusions_and_ssg_and_context};
    use pointer::app::*;
    use sqlx::postgres::PgPoolOptions;

    let pool = PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.database_url)
        .await
        .context("failed to connect to postgres")?;

    let state = Arc::new(Mutex::new(pointer::server::AppState { pool }));
    let file_state = state.clone();
    let render_state = state.clone();

    tracing::info!("Starting pointer web UI");

    let conf = get_configuration(Some("Cargo.toml")).expect("Failed to read configuration");
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    let shell_options = leptos_options.clone();
    // Generate the list of routes in your Leptos App

    let context = move || {
        provide_context(render_state.clone());
    };

    let routes =
        generate_route_list_with_exclusions_and_ssg_and_context(App, None, context.clone()).0;
    let app = Router::new()
        .leptos_routes_with_context(&leptos_options, routes, context.clone(), move || {
            let val = shell_options.clone();
            move || shell(val.clone())
        })
        .fallback(leptos_axum::file_and_error_handler_with_context(
            move || provide_context(file_state.clone()),
            shell,
        ))
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
