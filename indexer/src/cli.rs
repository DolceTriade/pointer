use std::env;
use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{ArgAction, Args, Parser, Subcommand};
use tracing::info;

use crate::config::IndexerConfig;
use crate::engine::Indexer;
use crate::output;
use crate::upload;
use crate::utils;

#[derive(Debug, Parser)]
#[command(
    name = "pointer-indexer",
    version,
    about = "Pointer indexing and query CLI"
)]
pub struct Cli {
    /// Increase logging verbosity (use -vv for trace level).
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Index a repository and produce/upload search metadata.
    Index(IndexArgs),
}

#[derive(Debug, Args)]
pub struct IndexArgs {
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
    /// URL of the backend ingestion endpoint. When provided, the generated index will be uploaded.
    #[arg(long)]
    pub upload_url: Option<String>,
    /// API key used when uploading to the backend (sent as a Bearer token).
    #[arg(long)]
    pub upload_api_key: Option<String>,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    utils::init_tracing(cli.verbose)?;

    match cli.command {
        Commands::Index(args) => run_index(args),
    }
}

fn run_index(args: IndexArgs) -> Result<()> {
    let repo_path = resolve_repo_path(&args.repo_path)?;
    let repository = args
        .repository
        .clone()
        .unwrap_or_else(|| utils::default_repo_name(&repo_path));
    let output_dir = resolve_output_dir(&args.output_dir)?;

    let repo_meta =
        utils::resolve_repo_metadata(&repo_path, args.commit.clone(), args.branch.clone())?;

    let config = IndexerConfig::new(
        repo_path.clone(),
        repository.clone(),
        repo_meta.branch,
        repo_meta.commit,
        output_dir.clone(),
    );

    let indexer = Indexer::new(config);
    let artifacts = indexer.run()?;
    output::write_report(&output_dir, &artifacts)?;

    if let Some(url) = args.upload_url.as_deref() {
        info!(%url, "uploading index to backend");
        upload::upload_index(url, args.upload_api_key.as_deref(), &artifacts)?;
    }

    info!(repo = repository, output = ?output_dir, files = artifacts.file_pointer_count(), "indexing complete");

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
