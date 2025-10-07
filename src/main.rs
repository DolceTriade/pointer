#[cfg(feature = "ssr")]
#[tokio::main]
async fn main() {
    // Initialize logging system with colored output
    #[cfg(feature = "ssr")]
    {
        use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        use tracing_subscriber::{EnvFilter, fmt};

        let fmt_layer = fmt::layer()
            .with_ansi(true) // Enable colored output
            .with_line_number(true)
            .with_file(true)
            .with_thread_ids(false)
            .with_thread_names(true);

        let filter_layer =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        tracing_subscriber::registry()
            .with(filter_layer)
            .with(fmt_layer)
            .init();
    }

    use axum::Router;
    use leptos::prelude::*;
    use leptos_axum::{LeptosRoutes, generate_route_list};
    use pointer::app::*;

    tracing::info!("Starting pointer web UI");

    let conf = get_configuration(Some("Cargo.toml")).expect("Failed to read configuration");
    let addr = conf.leptos_options.site_addr;
    let leptos_options = conf.leptos_options;
    let shell_options = leptos_options.clone();
    // Generate the list of routes in your Leptos App
    let routes = generate_route_list(App);

    let app = Router::new()
        .leptos_routes(&leptos_options, routes, move || {
            let val = shell_options.clone();
            move || shell(val.clone())
        })
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options);

    tracing::info!("listening on http://{}", &addr);

    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                eprintln!("Server error: {}", e);
            }
        }
        Err(e) => {
            eprintln!("Failed to bind to address {}: {}", addr, e);
        }
    }
}

#[cfg(not(feature = "ssr"))]
pub fn main() {
    // no client-side main function
    // unless we want this to work with e.g., Trunk for pure client-side testing
    // see lib.rs for hydration function instead
}
