use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::process::Command;
use tokio::sync::{Mutex, Semaphore};
use tracing::{error, info};

use crate::config::{AppConfig, RepoConfig};
use crate::git::{Git, RepoPaths};
use crate::hooks;
use crate::indexer;
use crate::state::PersistedState;

pub struct Scheduler {
    cfg: Arc<AppConfig>,
    git: Git,
    state_path: std::path::PathBuf,
    state: Arc<Mutex<PersistedState>>,
    semaphore: Arc<Semaphore>,
}

#[derive(Default)]
struct CycleStats {
    branches_total: usize,
    branches_changed: usize,
    branches_skipped_unchanged: usize,
    branches_succeeded: usize,
    branches_failed: usize,
}

enum BranchOutcome {
    SkippedUnchanged,
    Succeeded,
    Failed,
}

impl Scheduler {
    pub fn new(cfg: AppConfig) -> Result<Self> {
        let state_path = cfg.global.state_dir.join("state.json");
        let state = PersistedState::load(&state_path)?;

        Ok(Self {
            semaphore: Arc::new(Semaphore::new(cfg.global.max_repo_concurrency)),
            git: Git::new(cfg.global.git_bin.clone()),
            cfg: Arc::new(cfg),
            state_path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub async fn validate_runtime(&self) -> Result<()> {
        let start = Instant::now();
        info!(
            stage = "startup",
            event = "startup.runtime_validation.begin",
            repo_count = self.cfg.repos.len(),
            "starting runtime validation"
        );

        self.git.validate_binary_exists().await?;
        validate_binary_exists(&self.cfg.global.indexer_bin).await?;

        for repo in &self.cfg.repos {
            let repo_start = Instant::now();
            info!(
                stage = "startup",
                event = "repo.validate.begin",
                repo = %repo.name,
                url = %repo.url,
                branch_pattern_count = repo.branches.len(),
                "validating repository runtime prerequisites"
            );

            let paths = self.git.repo_paths(&self.cfg.global.state_dir, &repo.name);

            let op_start = Instant::now();
            info!(stage = "startup", event = "repo.validate.ensure_mirror.begin", repo = %repo.name, "ensuring mirror");
            match self.git.ensure_mirror(repo, &paths).await {
                Ok(()) => info!(
                    stage = "startup",
                    event = "repo.validate.ensure_mirror.end",
                    repo = %repo.name,
                    result = "ok",
                    duration_ms = op_start.elapsed().as_millis(),
                    "mirror ensured"
                ),
                Err(err) => {
                    error!(
                        stage = "startup",
                        event = "repo.validate.ensure_mirror.end",
                        repo = %repo.name,
                        result = "fail",
                        duration_ms = op_start.elapsed().as_millis(),
                        error = %format!("{err:#}"),
                        "failed to ensure mirror"
                    );
                    return Err(err);
                }
            }

            let op_start = Instant::now();
            info!(stage = "startup", event = "repo.validate.fetch.begin", repo = %repo.name, "validating fetch");
            match self.git.fetch_configured_patterns(repo, &paths).await {
                Ok(()) => info!(
                    stage = "startup",
                    event = "repo.validate.fetch.end",
                    repo = %repo.name,
                    result = "ok",
                    duration_ms = op_start.elapsed().as_millis(),
                    "fetch validated"
                ),
                Err(err) => {
                    error!(
                        stage = "startup",
                        event = "repo.validate.fetch.end",
                        repo = %repo.name,
                        result = "fail",
                        duration_ms = op_start.elapsed().as_millis(),
                        error = %format!("{err:#}"),
                        "failed to fetch during validation"
                    );
                    return Err(err);
                }
            }

            let op_start = Instant::now();
            info!(stage = "startup", event = "repo.validate.resolve_branches.begin", repo = %repo.name, "validating branch resolution");
            match self.git.resolve_branches(repo, &paths).await {
                Ok(resolved) => info!(
                    stage = "startup",
                    event = "repo.validate.resolve_branches.end",
                    repo = %repo.name,
                    result = "ok",
                    duration_ms = op_start.elapsed().as_millis(),
                    resolved_branch_count = resolved.len(),
                    "branch resolution validated"
                ),
                Err(err) => {
                    error!(
                        stage = "startup",
                        event = "repo.validate.resolve_branches.end",
                        repo = %repo.name,
                        result = "fail",
                        duration_ms = op_start.elapsed().as_millis(),
                        error = %format!("{err:#}"),
                        "failed branch resolution during validation"
                    );
                    return Err(err);
                }
            }

            info!(
                stage = "startup",
                event = "repo.validate.end",
                repo = %repo.name,
                result = "ok",
                duration_ms = repo_start.elapsed().as_millis(),
                "repository runtime validation completed"
            );
        }

        info!(
            stage = "startup",
            event = "startup.runtime_validation.end",
            result = "ok",
            duration_ms = start.elapsed().as_millis(),
            "runtime validation completed"
        );

        Ok(())
    }

    pub async fn run_once(&self) {
        info!(
            stage = "startup",
            event = "startup.ready",
            mode = "once",
            repo_count = self.cfg.repos.len(),
            "scheduler starting in once mode"
        );

        let mut handles = Vec::new();
        for repo in &self.cfg.repos {
            let repo = repo.clone();
            let this = self.clone();
            handles.push(tokio::spawn(async move {
                this.run_repo_cycle(repo).await;
            }));
        }

        for handle in handles {
            if let Err(err) = handle.await {
                error!(
                    stage = "cycle",
                    event = "cycle.join",
                    result = "fail",
                    error = %err,
                    "repo cycle task panicked or was cancelled"
                );
            }
        }

        let _ = self.run_global_finish_hook("once", 1).await;
    }

    pub async fn run_forever(&self) {
        info!(
            stage = "startup",
            event = "startup.ready",
            mode = "forever",
            repo_count = self.cfg.repos.len(),
            "scheduler starting in forever mode"
        );

        let mut next_due: HashMap<String, Instant> = self
            .cfg
            .repos
            .iter()
            .map(|repo| (repo.name.clone(), Instant::now()))
            .collect();

        let mut sweep_completed: HashSet<String> = HashSet::new();
        let mut sweep_id: u64 = 1;

        loop {
            let now = Instant::now();
            let mut due_repos = Vec::new();

            for repo in &self.cfg.repos {
                if let Some(next) = next_due.get(&repo.name) {
                    if *next <= now {
                        due_repos.push(repo.clone());
                    }
                }
            }

            if due_repos.is_empty() {
                let next_wake = next_due
                    .values()
                    .min()
                    .copied()
                    .unwrap_or_else(|| now + Duration::from_secs(1));

                tokio::select! {
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_wake)) => {}
                    _ = tokio::signal::ctrl_c() => {
                        info!(stage = "startup", event = "startup.shutdown", "received ctrl-c, shutting down");
                        return;
                    }
                }

                continue;
            }

            let mut handles = Vec::new();
            for repo in due_repos {
                next_due.insert(repo.name.clone(), Instant::now() + repo.interval);
                let repo_name = repo.name.clone();
                let this = self.clone();
                handles.push(tokio::spawn(async move {
                    this.run_repo_cycle(repo).await;
                    repo_name
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok(repo_name) => {
                        sweep_completed.insert(repo_name);
                    }
                    Err(err) => {
                        error!(
                            stage = "cycle",
                            event = "cycle.join",
                            result = "fail",
                            error = %err,
                            "repo cycle task panicked or was cancelled"
                        );
                    }
                }
            }

            if sweep_completed.len() == self.cfg.repos.len() {
                let _ = self.run_global_finish_hook("forever", sweep_id).await;
                sweep_completed.clear();
                sweep_id = sweep_id.saturating_add(1);
            }
        }
    }

    async fn run_repo_cycle(&self, repo: RepoConfig) {
        let wait_start = Instant::now();
        let permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            Err(_) => return,
        };

        let cycle_start = Instant::now();
        info!(
            stage = "cycle",
            event = "cycle.begin",
            repo = %repo.name,
            interval_secs = repo.interval.as_secs(),
            semaphore_wait_ms = wait_start.elapsed().as_millis(),
            "starting repo poll cycle"
        );

        let paths = self.git.repo_paths(&self.cfg.global.state_dir, &repo.name);

        let cycle_result = self.run_repo_cycle_inner(&repo, &paths).await;

        match cycle_result {
            Ok(stats) => {
                info!(
                    stage = "cycle",
                    event = "cycle.summary",
                    repo = %repo.name,
                    branches_total = stats.branches_total,
                    branches_changed = stats.branches_changed,
                    branches_skipped_unchanged = stats.branches_skipped_unchanged,
                    branches_succeeded = stats.branches_succeeded,
                    branches_failed = stats.branches_failed,
                    duration_ms = cycle_start.elapsed().as_millis(),
                    "cycle progress summary"
                );

                info!(
                    stage = "cycle",
                    event = "cycle.end",
                    repo = %repo.name,
                    result = "ok",
                    duration_ms = cycle_start.elapsed().as_millis(),
                    "poll cycle completed"
                );
            }
            Err(err) => error!(
                stage = "cycle",
                event = "cycle.end",
                repo = %repo.name,
                result = "fail",
                duration_ms = cycle_start.elapsed().as_millis(),
                error = %format!("{err:#}"),
                "poll cycle failed"
            ),
        }

        drop(permit);
    }

    async fn run_repo_cycle_inner(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
    ) -> Result<CycleStats> {
        let mut stats = CycleStats::default();

        let fetch_start = Instant::now();
        info!(
            stage = "cycle",
            event = "cycle.fetch.begin",
            repo = %repo.name,
            "starting branch fetch"
        );
        self.git
            .fetch_configured_patterns(repo, paths)
            .await
            .with_context(|| format!("fetch failed for repo '{}'", repo.name))?;
        info!(
            stage = "cycle",
            event = "cycle.fetch.end",
            repo = %repo.name,
            result = "ok",
            duration_ms = fetch_start.elapsed().as_millis(),
            "fetched configured branch refs"
        );

        let resolve_start = Instant::now();
        info!(
            stage = "cycle",
            event = "cycle.resolve.begin",
            repo = %repo.name,
            "resolving tracked branches"
        );
        let branches = self
            .git
            .resolve_branches(repo, paths)
            .await
            .with_context(|| format!("failed to resolve branches for repo '{}'", repo.name))?;
        stats.branches_total = branches.len();
        info!(
            stage = "cycle",
            event = "cycle.resolve.end",
            repo = %repo.name,
            result = "ok",
            duration_ms = resolve_start.elapsed().as_millis(),
            resolved_branch_count = branches.len(),
            "resolved tracked branches"
        );

        for (branch, commit) in branches {
            let outcome = self.process_branch(repo, paths, &branch, &commit).await;

            match outcome {
                BranchOutcome::SkippedUnchanged => {
                    stats.branches_skipped_unchanged += 1;
                }
                BranchOutcome::Succeeded => {
                    stats.branches_changed += 1;
                    stats.branches_succeeded += 1;
                }
                BranchOutcome::Failed => {
                    stats.branches_changed += 1;
                    stats.branches_failed += 1;
                }
            }
        }

        Ok(stats)
    }

    async fn process_branch(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
        branch: &str,
        commit: &str,
    ) -> BranchOutcome {
        let branch_start = Instant::now();
        info!(
            stage = "branch",
            event = "branch.begin",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            "starting branch processing"
        );

        let unchanged = {
            let state = self.state.lock().await;
            state.has_commit(&repo.name, branch, commit)
        };

        if unchanged {
            info!(
                stage = "branch",
                event = "branch.skip_unchanged",
                repo = %repo.name,
                branch = %branch,
                commit = %commit,
                "branch head unchanged; skipping index"
            );
            info!(
                stage = "branch",
                event = "branch.end",
                repo = %repo.name,
                branch = %branch,
                commit = %commit,
                result = "ok",
                duration_ms = branch_start.elapsed().as_millis(),
                "branch processing completed"
            );
            return BranchOutcome::SkippedUnchanged;
        }

        info!(
            stage = "branch",
            event = "branch.worktree.prepare.begin",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            "preparing branch worktree"
        );
        let worktree = match self
            .git
            .prepare_worktree(repo.name.as_str(), paths, branch, commit)
            .await
        {
            Ok(worktree) => {
                info!(
                    stage = "branch",
                    event = "branch.worktree.prepare.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "ok",
                    worktree = %worktree.display(),
                    "prepared branch worktree"
                );
                worktree
            }
            Err(err) => {
                error!(
                    stage = "branch",
                    event = "branch.worktree.prepare.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    error = %format!("{err:#}"),
                    "failed to prepare worktree"
                );
                info!(
                    stage = "branch",
                    event = "branch.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    duration_ms = branch_start.elapsed().as_millis(),
                    "branch processing failed"
                );
                return BranchOutcome::Failed;
            }
        };

        for (idx, hook) in repo.pre_index_hooks.iter().enumerate() {
            let hook_index = idx + 1;
            match hooks::run_hook(
                &self.cfg.global.shell,
                hook,
                "pre",
                hook_index,
                &repo.name,
                branch,
                commit,
                &worktree,
                &self.cfg.global.state_dir,
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    error!(
                        stage = "branch",
                        event = "branch.hooks.pre.end",
                        repo = %repo.name,
                        branch = %branch,
                        commit = %commit,
                        result = "fail",
                        hook_index,
                        error = %format!("{err:#}"),
                        "pre hook sequence failed"
                    );
                    info!(
                        stage = "branch",
                        event = "branch.end",
                        repo = %repo.name,
                        branch = %branch,
                        commit = %commit,
                        result = "fail",
                        duration_ms = branch_start.elapsed().as_millis(),
                        "branch processing failed"
                    );
                    return BranchOutcome::Failed;
                }
            }
        }

        let branch_indexer_args = repo
            .per_branch
            .iter()
            .find(|cfg| cfg.branch == branch)
            .map(|cfg| cfg.indexer_args.clone())
            .unwrap_or_default();

        info!(
            stage = "branch",
            event = "branch.index.begin",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            index_args_global_count = self.cfg.global.indexer_args.len(),
            index_args_repo_count = repo.indexer_args.len(),
            index_args_branch_count = branch_indexer_args.len(),
            "starting indexing for branch"
        );
        match indexer::run_indexer(
            &self.cfg.global.indexer_bin,
            &self.cfg.global.indexer_args,
            repo,
            &branch_indexer_args,
            branch,
            commit,
            &worktree,
        )
        .await
        {
            Ok(index_result) => {
                let msg = summarize_output(
                    "indexer completed",
                    &index_result.stdout,
                    &index_result.stderr,
                );
                info!(
                    stage = "branch",
                    event = "branch.index.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "ok",
                    duration_ms = index_result.duration.as_millis(),
                    message = %msg,
                    "indexing finished for branch"
                );
            }
            Err(err) => {
                error!(
                    stage = "branch",
                    event = "branch.index.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    error = %format!("{err:#}"),
                    "indexing failed for branch"
                );
                info!(
                    stage = "branch",
                    event = "branch.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    duration_ms = branch_start.elapsed().as_millis(),
                    "branch processing failed"
                );
                return BranchOutcome::Failed;
            }
        }

        for (idx, hook) in repo.post_upload_hooks.iter().enumerate() {
            let hook_index = idx + 1;
            match hooks::run_hook(
                &self.cfg.global.shell,
                hook,
                "post",
                hook_index,
                &repo.name,
                branch,
                commit,
                &worktree,
                &self.cfg.global.state_dir,
            )
            .await
            {
                Ok(_) => {}
                Err(err) => {
                    error!(
                        stage = "branch",
                        event = "branch.hooks.post.end",
                        repo = %repo.name,
                        branch = %branch,
                        commit = %commit,
                        result = "fail",
                        hook_index,
                        error = %format!("{err:#}"),
                        "post hook sequence failed"
                    );
                    info!(
                        stage = "branch",
                        event = "branch.end",
                        repo = %repo.name,
                        branch = %branch,
                        commit = %commit,
                        result = "fail",
                        duration_ms = branch_start.elapsed().as_millis(),
                        "branch processing failed"
                    );
                    return BranchOutcome::Failed;
                }
            }
        }

        info!(
            stage = "branch",
            event = "branch.state.save.begin",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            "persisting branch state"
        );
        {
            let mut state = self.state.lock().await;
            state.update_success(&repo.name, branch, commit);
            if let Err(err) = state.save(&self.state_path) {
                error!(
                    stage = "branch",
                    event = "branch.state.save.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    error = %format!("{err:#}"),
                    "failed to persist branch state"
                );
                info!(
                    stage = "branch",
                    event = "branch.end",
                    repo = %repo.name,
                    branch = %branch,
                    commit = %commit,
                    result = "fail",
                    duration_ms = branch_start.elapsed().as_millis(),
                    "branch processing failed"
                );
                return BranchOutcome::Failed;
            }
        }

        info!(
            stage = "branch",
            event = "branch.state.save.end",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            result = "ok",
            "persisted branch state"
        );

        info!(
            stage = "branch",
            event = "branch.end",
            repo = %repo.name,
            branch = %branch,
            commit = %commit,
            result = "ok",
            duration_ms = branch_start.elapsed().as_millis(),
            "branch processing completed"
        );

        BranchOutcome::Succeeded
    }

    async fn run_global_finish_hook(&self, mode: &str, sweep_id: u64) -> Result<()> {
        let Some(hook) = self.cfg.global.finish_hook.as_ref() else {
            return Ok(());
        };

        let start = Instant::now();
        info!(
            stage = "global_hook",
            event = "global.finish_hook.begin",
            mode = %mode,
            sweep_id,
            command = %hook.command,
            "running global finish hook"
        );

        match hooks::run_hook(
            &self.cfg.global.shell,
            hook,
            "global_finish",
            1,
            "__global__",
            "__sweep__",
            "__none__",
            self.cfg.global.state_dir.as_path(),
            self.cfg.global.state_dir.as_path(),
        )
        .await
        {
            Ok(result) => {
                info!(
                    stage = "global_hook",
                    event = "global.finish_hook.end",
                    result = "ok",
                    mode = %mode,
                    sweep_id,
                    duration_ms = start.elapsed().as_millis(),
                    hook_duration_ms = result.duration.as_millis(),
                    status_code = ?result.status_code,
                    stdout = %result.stdout,
                    stderr = %result.stderr,
                    "global finish hook completed"
                );
            }
            Err(err) => {
                error!(
                    stage = "global_hook",
                    event = "global.finish_hook.end",
                    result = "fail",
                    mode = %mode,
                    sweep_id,
                    duration_ms = start.elapsed().as_millis(),
                    error = %format!("{err:#}"),
                    "global finish hook failed; continuing"
                );
            }
        }

        Ok(())
    }
}

impl Clone for Scheduler {
    fn clone(&self) -> Self {
        Self {
            cfg: Arc::clone(&self.cfg),
            git: self.git.clone(),
            state_path: self.state_path.clone(),
            state: Arc::clone(&self.state),
            semaphore: Arc::clone(&self.semaphore),
        }
    }
}

fn summarize_output(prefix: &str, stdout: &str, stderr: &str) -> String {
    let out = stdout.lines().last().unwrap_or("").trim();
    let err = stderr.lines().last().unwrap_or("").trim();

    if !err.is_empty() {
        format!("{prefix}; stderr={err}")
    } else if !out.is_empty() {
        format!("{prefix}; stdout={out}")
    } else {
        prefix.to_string()
    }
}

async fn validate_binary_exists(bin: &str) -> Result<()> {
    info!(
        stage = "startup",
        event = "startup.binary_check.begin",
        binary = %bin,
        "checking binary availability"
    );

    let status = Command::new(bin)
        .arg("--version")
        .status()
        .await
        .with_context(|| format!("failed to check binary '{}'", bin))?;

    if !status.success() {
        error!(
            stage = "startup",
            event = "startup.binary_check.end",
            binary = %bin,
            result = "fail",
            status_code = ?status.code(),
            "binary check failed"
        );
        anyhow::bail!("binary '{}' is not available in PATH", bin);
    }

    info!(
        stage = "startup",
        event = "startup.binary_check.end",
        binary = %bin,
        result = "ok",
        status_code = ?status.code(),
        "binary check succeeded"
    );

    Ok(())
}
