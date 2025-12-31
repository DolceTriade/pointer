use std::env;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Result;
use clap::{ArgAction, Args, Parser, Subcommand};
use humantime::parse_duration;
use tracing::info;

use crate::admin;
use crate::config::{BranchPolicyConfig, IndexerConfig, SnapshotPolicyConfig};
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
    /// Administrative actions against the backend service.
    Admin(AdminArgs),
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
    /// Mark this branch as the live branch for the repository.
    #[arg(long = "live", action = ArgAction::SetTrue, conflicts_with = "not_live")]
    pub live: bool,
    /// Explicitly mark this branch as not-live.
    #[arg(long = "not-live", action = ArgAction::SetTrue, conflicts_with = "live")]
    pub not_live: bool,
    /// Number of most recent snapshots that should always be retained.
    #[arg(long = "keep-latest", default_value_t = 1)]
    pub keep_latest: u32,
    /// Snapshot retention policies in the format "<interval>:<count>", e.g. "7d:4".
    #[arg(long = "snapshot-policy")]
    pub snapshot_policies: Vec<SnapshotPolicyArg>,
}

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    utils::init_tracing(cli.verbose)?;

    match cli.command {
        Commands::Index(args) => run_index(args),
        Commands::Admin(args) => admin::run_admin(args),
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
        build_branch_policy(&args),
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

fn build_branch_policy(args: &IndexArgs) -> Option<BranchPolicyConfig> {
    let branch = args.branch.as_ref()?;
    if branch.trim().is_empty() {
        return None;
    }

    let live = if args.live {
        Some(true)
    } else if args.not_live {
        Some(false)
    } else {
        None
    };

    let latest_keep = args.keep_latest.max(1);
    let snapshot_policies = args
        .snapshot_policies
        .iter()
        .map(|policy| SnapshotPolicyConfig {
            interval_seconds: policy.interval_seconds,
            keep_count: policy.keep_count,
        })
        .collect();

    Some(BranchPolicyConfig {
        live,
        latest_keep_count: latest_keep,
        snapshot_policies,
    })
}

#[derive(Debug, Clone)]
pub struct SnapshotPolicyArg {
    pub interval_seconds: u64,
    pub keep_count: u32,
}

impl FromStr for SnapshotPolicyArg {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (interval_part, count_part) = input
            .split_once(':')
            .ok_or_else(|| "snapshot policy must be in the form <interval>:<count>".to_string())?;

        let duration = parse_duration(interval_part)
            .map_err(|err| format!("invalid interval '{interval_part}': {err}"))?;
        let interval_seconds = duration.as_secs();
        if interval_seconds == 0 {
            return Err("snapshot policy interval must be greater than zero".to_string());
        }

        let keep_count: u32 = count_part
            .parse()
            .map_err(|_| format!("invalid snapshot count '{count_part}'"))?;
        if keep_count == 0 {
            return Err("snapshot policy count must be greater than zero".to_string());
        }

        Ok(Self {
            interval_seconds,
            keep_count,
        })
    }
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

#[derive(Debug, Args)]
pub struct AdminArgs {
    /// Base URL for the backend admin API (e.g. http://localhost:8080/api/v1).
    #[arg(long, env = "POINTER_BACKEND_URL")]
    pub backend_url: Option<String>,
    /// API key used when calling the backend (sent as a Bearer token).
    #[arg(long)]
    pub api_key: Option<String>,
    #[command(subcommand)]
    pub command: AdminCommand,
}

#[derive(Debug, Subcommand)]
pub enum AdminCommand {
    /// Run garbage collection.
    Gc,
    /// Rebuild the symbol name cache.
    RebuildSymbolCache,
    /// Cleanup orphaned symbol cache rows.
    CleanupSymbolCache(CleanupSymbolCacheArgs),
    /// Incrementally refresh unique symbol names.
    RefreshSymbolCache(RefreshSymbolCacheArgs),
    /// Prune all data for a specific commit.
    PruneCommit(PruneCommitArgs),
    /// Prune all historical commits for a branch (keeps latest).
    PruneBranch(PruneBranchArgs),
    /// Prune all data for a repository.
    PruneRepo(PruneRepoArgs),
    /// Apply retention policy for a repository.
    PrunePolicy(PrunePolicyArgs),
}

#[derive(Debug, Args)]
pub struct PruneCommitArgs {
    #[arg(long)]
    pub repository: String,
    #[arg(long)]
    pub commit_sha: String,
}

#[derive(Debug, Args)]
pub struct PruneBranchArgs {
    #[arg(long)]
    pub repository: String,
    #[arg(long)]
    pub branch: String,
}

#[derive(Debug, Args)]
pub struct PruneRepoArgs {
    #[arg(long)]
    pub repository: String,
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
}

#[derive(Debug, Args)]
pub struct PrunePolicyArgs {
    #[arg(long)]
    pub repository: String,
    #[arg(long, default_value_t = true)]
    pub keep_latest: bool,
    #[arg(long)]
    pub max_commits_to_keep: Option<i32>,
}

#[derive(Debug, Args)]
pub struct CleanupSymbolCacheArgs {
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
    #[arg(long, default_value_t = 50)]
    pub max_batches: i64,
}

#[derive(Debug, Args)]
pub struct RefreshSymbolCacheArgs {
    #[arg(long, default_value_t = 10_000)]
    pub batch_size: i64,
    #[arg(long, default_value_t = 0)]
    pub max_batches: i64,
}
