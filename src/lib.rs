#![recursion_limit = "256"]
pub mod app;
pub mod components;
pub mod db;
pub mod dsl;
pub mod pages;
pub mod services;

#[cfg(feature = "ssr")]
pub mod server;

#[cfg(feature = "hydrate")]
use wasm_bindgen::prelude::*;

#[cfg(feature = "hydrate")]
#[wasm_bindgen]
pub fn hydrate() {
    use crate::app::*;
    use tracing_subscriber::prelude::*;
    use tracing_web::MakeWebConsoleWriter;
    console_error_panic_hook::set_once();

    // Initialize console logging for WASM
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false) // Only partially supported across browsers
        .without_time() // std::time is not available in browsers, see note below
        .with_writer(MakeWebConsoleWriter::new()); // write events to the console
    tracing_subscriber::registry().with(fmt_layer).init();

    leptos::mount::hydrate_body(App);
}
