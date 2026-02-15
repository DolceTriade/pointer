mod config;
mod git;
mod hooks;
mod indexer;
mod logging;
mod scheduler;
mod state;

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use config::AppConfig;
use logging::init_logging;
use scheduler::Scheduler;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(
    name = "pointer-reposerver",
    version,
    about = "Poll and index git repositories"
)]
struct Cli {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value_t = false)]
    once: bool,
    #[arg(long, default_value_t = false)]
    validate_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    init_logging()?;
    info!(
        stage = "startup",
        event = "startup.begin",
        config_path = %cli.config.display(),
        "pointer-reposerver process starting"
    );

    let load_start = std::time::Instant::now();
    info!(
        stage = "startup",
        event = "config.load.begin",
        config_path = %cli.config.display(),
        "loading configuration file"
    );
    let cfg = match AppConfig::load(&cli.config).context("failed to load pointer-reposerver config")
    {
        Ok(cfg) => {
            info!(
                stage = "startup",
                event = "config.load.end",
                result = "ok",
                config_path = %cli.config.display(),
                repo_count = cfg.repos.len(),
                duration_ms = load_start.elapsed().as_millis(),
                "configuration loaded"
            );
            cfg
        }
        Err(err) => {
            error!(
                stage = "startup",
                event = "config.load.end",
                result = "fail",
                config_path = %cli.config.display(),
                duration_ms = load_start.elapsed().as_millis(),
                error = %format!("{err:#}"),
                "configuration load failed"
            );
            return Err(err);
        }
    };

    let validate_start = std::time::Instant::now();
    info!(
        stage = "startup",
        event = "config.validate.begin",
        repo_count = cfg.repos.len(),
        "validating configuration"
    );
    if let Err(err) = cfg.validate_config() {
        error!(
            stage = "startup",
            event = "config.validate.end",
            result = "fail",
            duration_ms = validate_start.elapsed().as_millis(),
            error = %format!("{err:#}"),
            "configuration validation failed"
        );
        return Err(err);
    }
    info!(
        stage = "startup",
        event = "config.validate.end",
        result = "ok",
        duration_ms = validate_start.elapsed().as_millis(),
        "configuration validation completed"
    );

    let scheduler = Scheduler::new(cfg)?;

    scheduler
        .validate_runtime()
        .await
        .context("runtime validation failed")?;

    if cli.validate_config {
        info!(
            stage = "startup",
            event = "startup.validate_only.exit",
            result = "ok",
            "configuration and runtime validation passed"
        );
        return Ok(());
    }

    if cli.once {
        info!(
            stage = "startup",
            event = "startup.mode",
            mode = "once",
            "running once"
        );
        scheduler.run_once().await;
    } else {
        info!(
            stage = "startup",
            event = "startup.mode",
            mode = "forever",
            "running continuously"
        );
        scheduler.run_forever().await;
    }

    Ok(())
}
