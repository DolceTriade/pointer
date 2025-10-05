use std::env;
use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{ArgAction, Parser};
use tracing::info;

use crate::config::IndexerConfig;
use crate::engine::Indexer;
use crate::output;
use crate::utils;

#[derive(Debug, Parser)]
#[command(
    name = "pointer-indexer",
    version,
    about = "Pointer content-aware indexer"
)]
pub struct Cli {
    /// Human-readable repository identifier (defaults to the repo directory name).
    #[arg(long, env = "POINTER_REPOSITORY")]
    pub repository: Option<String>,
    /// Path to the repository root to index.
    #[arg(long = "repo", default_value = ".")]
    pub repo_path: PathBuf,
    /// Commit SHA to associate with the produced metadata. Defaults to HEAD.
    #[arg(long)]
    pub commit: Option<String>,
    /// Branch name associated with the commit. Defaults to the current branch when available.
    #[arg(long)]
    pub branch: Option<String>,
    /// Directory where JSON artifacts will be written.
    #[arg(long, default_value = "index-output")]
    pub output_dir: PathBuf,
    /// Increase logging verbosity (use -vv for trace level).
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    utils::init_tracing(cli.verbose)?;

    let repo_path = resolve_repo_path(&cli.repo_path)?;
    let repository = cli
        .repository
        .clone()
        .unwrap_or_else(|| utils::default_repo_name(&repo_path));
    let output_dir = resolve_output_dir(&cli.output_dir)?;

    let repo_meta =
        utils::resolve_repo_metadata(&repo_path, cli.commit.clone(), cli.branch.clone())?;

    let config = IndexerConfig::new(
        repo_path.clone(),
        repository.clone(),
        repo_meta.branch,
        repo_meta.commit,
        output_dir.clone(),
    );

    let indexer = Indexer::new(config);
    let report = indexer.run()?;
    output::write_report(&output_dir, &report)?;

    info!(
        repo = repository,
        output = ?output_dir,
        files = report.file_pointers.len(),
        "indexing complete"
    );

    Ok(())
}

fn resolve_repo_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

fn resolve_output_dir(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}
