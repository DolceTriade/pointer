use anyhow::{Context, Result};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

pub fn init_logging() -> Result<()> {
    let filter_layer = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = fmt::layer()
        .compact()
        .with_ansi(false)
        .with_target(false)
        .boxed();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .context("failed to initialize tracing subscriber")?;

    Ok(())
}
