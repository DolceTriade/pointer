use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::process::Command;
use tracing::{error, info};

use crate::config::HookConfig;

#[derive(Debug)]
pub struct HookResult {
    pub duration: Duration,
    pub status_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

pub async fn run_hook(
    hook: &HookConfig,
    hook_type: &str,
    hook_index: usize,
    repo: &str,
    branch: &str,
    commit: &str,
    worktree_path: &Path,
    state_dir: &Path,
) -> Result<HookResult> {
    info!(
        stage = "hook",
        event = "hook.begin",
        hook_type = %hook_type,
        hook_index,
        repo = %repo,
        branch = %branch,
        commit = %commit,
        command = %hook.command,
        "starting hook command"
    );

    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(&hook.command);
    cmd.env("REPOSERVER_REPO", repo);
    cmd.env("REPOSERVER_BRANCH", branch);
    cmd.env("REPOSERVER_COMMIT", commit);
    cmd.env("REPOSERVER_WORKTREE", worktree_path);
    cmd.env("REPOSERVER_STATE_DIR", state_dir);

    let start = Instant::now();

    let output = if let Some(timeout) = hook.timeout {
        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(output) => output.context("failed to execute hook")?,
            Err(_) => {
                error!(
                    stage = "hook",
                    event = "hook.end",
                    result = "fail",
                    hook_type = %hook_type,
                    hook_index,
                    repo = %repo,
                    branch = %branch,
                    commit = %commit,
                    timeout_secs = timeout.as_secs(),
                    command = %hook.command,
                    "hook timed out"
                );
                return Err(anyhow!(
                    "hook timed out after {}s: {}",
                    timeout.as_secs(),
                    hook.command
                ));
            }
        }
    } else {
        cmd.output().await.context("failed to execute hook")?
    };

    let result = HookResult {
        duration: start.elapsed(),
        status_code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).trim().to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).trim().to_string(),
    };

    if !output.status.success() {
        error!(
            stage = "hook",
            event = "hook.end",
            result = "fail",
            hook_type = %hook_type,
            hook_index,
            repo = %repo,
            branch = %branch,
            commit = %commit,
            duration_ms = result.duration.as_millis(),
            status_code = ?result.status_code,
            command = %hook.command,
            stderr = %result.stderr,
            "hook command failed"
        );
        bail!(
            "hook failed with status {:?}: {}",
            result.status_code,
            hook.command
        );
    }

    info!(
        stage = "hook",
        event = "hook.end",
        result = "ok",
        hook_type = %hook_type,
        hook_index,
        repo = %repo,
        branch = %branch,
        commit = %commit,
        duration_ms = result.duration.as_millis(),
        status_code = ?result.status_code,
        stdout = %result.stdout,
        stderr = %result.stderr,
        "hook command completed"
    );

    Ok(result)
}
