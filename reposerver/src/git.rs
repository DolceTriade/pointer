use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use glob::Pattern;
use tokio::process::Command;
use tracing::{error, info};

use crate::config::RepoConfig;

#[derive(Debug, Clone)]
pub struct Git {
    bin: String,
}

#[derive(Debug, Clone)]
pub struct RepoPaths {
    pub mirror: PathBuf,
    pub worktrees_root: PathBuf,
}

impl Git {
    pub fn new(bin: impl Into<String>) -> Self {
        Self { bin: bin.into() }
    }

    pub fn repo_paths(&self, state_dir: &Path, repo_name: &str) -> RepoPaths {
        let root = state_dir.join("repos").join(repo_name);
        RepoPaths {
            mirror: root.join("mirror.git"),
            worktrees_root: root.join("worktrees"),
        }
    }

    pub async fn validate_binary_exists(&self) -> Result<()> {
        info!(
            stage = "git",
            event = "git.binary_check.begin",
            git_bin = %self.bin,
            "checking git binary availability"
        );

        let status = Command::new(&self.bin)
            .arg("--version")
            .status()
            .await
            .with_context(|| format!("failed to check binary '{}'", self.bin))?;

        if !status.success() {
            error!(
                stage = "git",
                event = "git.binary_check.end",
                result = "fail",
                git_bin = %self.bin,
                status_code = ?status.code(),
                "git binary check failed"
            );
            bail!("binary '{}' is not available in PATH", self.bin);
        }

        info!(
            stage = "git",
            event = "git.binary_check.end",
            result = "ok",
            git_bin = %self.bin,
            status_code = ?status.code(),
            "git binary check succeeded"
        );

        Ok(())
    }

    pub async fn ensure_mirror(&self, repo: &RepoConfig, paths: &RepoPaths) -> Result<()> {
        if paths.mirror.exists() {
            info!(
                stage = "git",
                event = "git.ensure_mirror.skip",
                repo = %repo.name,
                mirror = %paths.mirror.display(),
                "mirror already exists"
            );
            return Ok(());
        }

        if let Some(parent) = paths.mirror.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create directory {}", parent.display()))?;
        }

        let mirror_path = paths.mirror.display().to_string();
        self.run(
            ["init", "--bare", mirror_path.as_str()],
            None,
            "ensure_mirror.init_bare",
            Some(repo.name.as_str()),
            None,
        )
        .await
        .with_context(|| {
            format!(
                "failed to initialize bare repository for '{}' at {}",
                repo.name, mirror_path
            )
        })?;

        self.run(
            [
                "--git-dir",
                mirror_path.as_str(),
                "remote",
                "add",
                "origin",
                repo.url.as_str(),
            ],
            None,
            "ensure_mirror.remote_add",
            Some(repo.name.as_str()),
            None,
        )
        .await
        .with_context(|| {
            format!(
                "failed to add origin remote for repo '{}' ({})",
                repo.name, repo.url
            )
        })?;

        let first_pattern = repo
            .branches
            .first()
            .ok_or_else(|| anyhow!("repo '{}' has no branches configured", repo.name))?
            .clone();

        self.fetch_patterns(
            paths,
            std::slice::from_ref(&first_pattern),
            "ensure_mirror.fetch_first_pattern",
            Some(repo.name.as_str()),
        )
        .await
        .with_context(|| {
            format!(
                "failed initial shallow fetch for first branch pattern '{}' in repo '{}'",
                first_pattern, repo.name
            )
        })?;

        if repo.branches.len() > 1 {
            self.fetch_patterns(
                paths,
                &repo.branches[1..],
                "ensure_mirror.fetch_remaining_patterns",
                Some(repo.name.as_str()),
            )
            .await
            .with_context(|| {
                format!(
                    "failed initial shallow fetch for remaining branches in repo '{}'",
                    repo.name
                )
            })?;
        }

        Ok(())
    }

    pub async fn fetch_configured_patterns(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
    ) -> Result<()> {
        self.fetch_patterns(
            paths,
            &repo.branches,
            "fetch_configured_patterns",
            Some(repo.name.as_str()),
        )
        .await
        .with_context(|| format!("git fetch failed for repo '{}'", repo.name))?;
        Ok(())
    }

    async fn fetch_patterns(
        &self,
        paths: &RepoPaths,
        patterns: &[String],
        operation: &str,
        repo: Option<&str>,
    ) -> Result<()> {
        if patterns.is_empty() {
            return Ok(());
        }

        let mut args = vec![
            "--git-dir".to_string(),
            paths.mirror.display().to_string(),
            "fetch".to_string(),
            "--prune".to_string(),
            "--no-tags".to_string(),
            "--depth=1".to_string(),
            "origin".to_string(),
        ];

        for branch_pattern in patterns {
            let refspec = format!(
                "+refs/heads/{0}:refs/remotes/origin/{0}",
                branch_pattern.trim()
            );
            args.push(refspec);
        }

        self.run(args, None, operation, repo, None).await
    }

    pub async fn resolve_branches(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
    ) -> Result<BTreeMap<String, String>> {
        let branches = self.list_remote_branches(paths, repo).await?;
        if branches.is_empty() {
            bail!("repo '{}' has no fetched remote branches", repo.name);
        }

        let mut wanted = BTreeSet::new();

        for configured in &repo.branches {
            if is_glob_pattern(configured) {
                let pattern = Pattern::new(configured).with_context(|| {
                    format!(
                        "repo '{}' has invalid branch glob pattern '{}'",
                        repo.name, configured
                    )
                })?;
                for branch in &branches {
                    if pattern.matches(branch) {
                        wanted.insert(branch.clone());
                    }
                }
            } else if branches.contains(configured) {
                wanted.insert(configured.to_string());
            }
        }

        if wanted.is_empty() {
            bail!(
                "repo '{}' branch patterns {:?} matched no remote branches",
                repo.name,
                repo.branches
            );
        }

        let mut heads = BTreeMap::new();
        for branch in wanted {
            let sha = self
                .remote_head_commit(paths, repo.name.as_str(), &branch)
                .await?;
            heads.insert(branch, sha);
        }

        Ok(heads)
    }

    pub async fn prepare_worktree(
        &self,
        repo_name: &str,
        paths: &RepoPaths,
        branch: &str,
        commit: &str,
    ) -> Result<PathBuf> {
        std::fs::create_dir_all(&paths.worktrees_root).with_context(|| {
            format!(
                "failed to create worktrees directory {}",
                paths.worktrees_root.display()
            )
        })?;

        let worktree = paths.worktrees_root.join(sanitize_branch(branch));

        if !worktree.exists() {
            self.run(
                vec![
                    "--git-dir".to_string(),
                    paths.mirror.display().to_string(),
                    "worktree".to_string(),
                    "add".to_string(),
                    "--detach".to_string(),
                    worktree.display().to_string(),
                    commit.to_string(),
                ],
                None,
                "prepare_worktree.add",
                Some(repo_name),
                Some(branch),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to create worktree {} for branch {}",
                    worktree.display(),
                    branch
                )
            })?;
        }

        self.run(
            ["checkout", "--detach", commit],
            Some(&worktree),
            "prepare_worktree.checkout",
            Some(repo_name),
            Some(branch),
        )
        .await
        .with_context(|| {
            format!(
                "failed to checkout commit {} in worktree {}",
                commit,
                worktree.display()
            )
        })?;

        self.run(
            ["reset", "--hard", commit],
            Some(&worktree),
            "prepare_worktree.reset_hard",
            Some(repo_name),
            Some(branch),
        )
        .await
        .with_context(|| {
            format!(
                "failed to hard reset commit {} in worktree {}",
                commit,
                worktree.display()
            )
        })?;

        Ok(worktree)
    }

    async fn list_remote_branches(
        &self,
        paths: &RepoPaths,
        repo: &RepoConfig,
    ) -> Result<BTreeSet<String>> {
        let mirror = paths.mirror.display().to_string();
        let output = self
            .run_capture(
                [
                    "--git-dir",
                    mirror.as_str(),
                    "for-each-ref",
                    "--format=%(refname:lstrip=3)",
                    "refs/remotes/origin",
                ],
                None,
                "list_remote_branches",
                Some(repo.name.as_str()),
                None,
            )
            .await?;

        let mut branches = BTreeSet::new();
        for line in output.lines() {
            let branch = line.trim();
            if branch.is_empty() || branch == "HEAD" {
                continue;
            }
            branches.insert(branch.to_string());
        }

        Ok(branches)
    }

    async fn remote_head_commit(
        &self,
        paths: &RepoPaths,
        repo_name: &str,
        branch: &str,
    ) -> Result<String> {
        let mirror = paths.mirror.display().to_string();
        let refname = format!("refs/remotes/origin/{branch}^{{commit}}");

        let output = self
            .run_capture(
                ["--git-dir", mirror.as_str(), "rev-parse", refname.as_str()],
                None,
                "remote_head_commit",
                Some(repo_name),
                Some(branch),
            )
            .await?;

        Ok(output.trim().to_string())
    }

    async fn run<I, S>(
        &self,
        args: I,
        cwd: Option<&Path>,
        operation: &str,
        repo: Option<&str>,
        branch: Option<&str>,
    ) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec = args
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect::<Vec<_>>();

        let cmd_display = format!("{} {}", self.bin, args_vec.join(" "));
        info!(
            stage = "git",
            event = "git.cmd.begin",
            operation = %operation,
            repo = ?repo,
            branch = ?branch,
            cwd = ?cwd.map(|p| p.display().to_string()),
            command = %cmd_display,
            "starting git command"
        );

        let mut cmd = Command::new(&self.bin);
        cmd.args(&args_vec);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }

        let start = Instant::now();
        let output = cmd
            .output()
            .await
            .with_context(|| format!("failed to execute '{} {}'", self.bin, args_vec.join(" ")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                stage = "git",
                event = "git.cmd.end",
                result = "fail",
                operation = %operation,
                repo = ?repo,
                branch = ?branch,
                duration_ms = start.elapsed().as_millis(),
                status_code = ?output.status.code(),
                stderr = %stderr.trim(),
                command = %cmd_display,
                "git command failed"
            );
            return Err(anyhow!(
                "git command failed (status {:?}): {}",
                output.status.code(),
                stderr.trim()
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stdout_tail = stdout.lines().last().unwrap_or("").trim();
        info!(
            stage = "git",
            event = "git.cmd.end",
            result = "ok",
            operation = %operation,
            repo = ?repo,
            branch = ?branch,
            duration_ms = start.elapsed().as_millis(),
            status_code = ?output.status.code(),
            stdout_tail = %stdout_tail,
            command = %cmd_display,
            "git command completed"
        );

        Ok(())
    }

    async fn run_capture<I, S>(
        &self,
        args: I,
        cwd: Option<&Path>,
        operation: &str,
        repo: Option<&str>,
        branch: Option<&str>,
    ) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let args_vec = args
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect::<Vec<_>>();

        let cmd_display = format!("{} {}", self.bin, args_vec.join(" "));
        info!(
            stage = "git",
            event = "git.cmd.begin",
            operation = %operation,
            repo = ?repo,
            branch = ?branch,
            cwd = ?cwd.map(|p| p.display().to_string()),
            command = %cmd_display,
            "starting git command"
        );

        let mut cmd = Command::new(&self.bin);
        cmd.args(&args_vec);
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }

        let start = Instant::now();
        let output = cmd
            .output()
            .await
            .with_context(|| format!("failed to execute '{} {}'", self.bin, args_vec.join(" ")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                stage = "git",
                event = "git.cmd.end",
                result = "fail",
                operation = %operation,
                repo = ?repo,
                branch = ?branch,
                duration_ms = start.elapsed().as_millis(),
                status_code = ?output.status.code(),
                stderr = %stderr.trim(),
                command = %cmd_display,
                "git command failed"
            );
            return Err(anyhow!(
                "git command failed (status {:?}): {}",
                output.status.code(),
                stderr.trim()
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stdout_tail = stdout.lines().last().unwrap_or("").trim().to_string();
        info!(
            stage = "git",
            event = "git.cmd.end",
            result = "ok",
            operation = %operation,
            repo = ?repo,
            branch = ?branch,
            duration_ms = start.elapsed().as_millis(),
            status_code = ?output.status.code(),
            stdout_tail = %stdout_tail,
            command = %cmd_display,
            "git command completed"
        );

        Ok(stdout)
    }
}

fn is_glob_pattern(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

fn sanitize_branch(branch: &str) -> String {
    branch
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
