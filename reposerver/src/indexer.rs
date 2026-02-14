use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio::process::Command;
use tracing::{error, info};

use crate::config::RepoConfig;

#[derive(Debug)]
pub struct IndexerResult {
    pub duration: Duration,
    pub status_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

pub async fn run_indexer(
    indexer_bin: &str,
    global_indexer_args: &[String],
    repo: &RepoConfig,
    branch_indexer_args: &[String],
    branch: &str,
    commit: &str,
    worktree_path: &Path,
) -> Result<IndexerResult> {
    info!(
        stage = "index",
        event = "indexer.begin",
        repo = %repo.name,
        branch = %branch,
        commit = %commit,
        indexer_bin = %indexer_bin,
        global_args_count = global_indexer_args.len(),
        repo_args_count = repo.indexer_args.len(),
        branch_args_count = branch_indexer_args.len(),
        "starting pointer-indexer process"
    );

    let mut cmd = Command::new(indexer_bin);
    cmd.arg("index");
    cmd.arg("--repo").arg(worktree_path);
    cmd.arg("--repository").arg(&repo.name);
    cmd.arg("--branch").arg(branch);
    cmd.arg("--commit").arg(commit);
    cmd.args(global_indexer_args);
    cmd.args(&repo.indexer_args);
    cmd.args(branch_indexer_args);

    let start = Instant::now();
    let output = cmd
        .output()
        .await
        .with_context(|| format!("failed to execute {}", indexer_bin))?;

    let result = IndexerResult {
        duration: start.elapsed(),
        status_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    };

    if !output.status.success() {
        error!(
            stage = "index",
            event = "indexer.end",
            result = "fail",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            duration_ms = result.duration.as_millis(),
            status_code = ?result.status_code,
            stderr = %result.stderr,
            "pointer-indexer process failed"
        );
        bail!(
            "indexer exited with status {:?} for repo={} branch={}",
            result.status_code,
            repo.name,
            branch
        );
    }

    info!(
        stage = "index",
        event = "indexer.end",
        result = "ok",
        repo = %repo.name,
        branch = %branch,
        commit = %commit,
        duration_ms = result.duration.as_millis(),
        status_code = ?result.status_code,
        stdout = %result.stdout,
        stderr = %result.stderr,
        "pointer-indexer process completed"
    );

    Ok(result)
}
