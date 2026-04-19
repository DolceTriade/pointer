use std::collections::{BTreeMap, BTreeSet};
use std::fs;
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

        Ok(())
    }

    pub fn clear_stale_index_locks(&self, repo: &RepoConfig, paths: &RepoPaths) -> Result<usize> {
        let mut removed = 0usize;

        let mirror_lock = paths.mirror.join("index.lock");
        removed += remove_lock_file(&mirror_lock)?;

        let worktrees_admin_root = paths.mirror.join("worktrees");
        if worktrees_admin_root.exists() {
            for entry in fs::read_dir(&worktrees_admin_root).with_context(|| {
                format!(
                    "failed to read git worktrees admin directory {}",
                    worktrees_admin_root.display()
                )
            })? {
                let entry = entry.with_context(|| {
                    format!(
                        "failed to read entry in git worktrees admin directory {}",
                        worktrees_admin_root.display()
                    )
                })?;
                let admin_dir = entry.path();
                if !admin_dir.is_dir() {
                    continue;
                }
                removed += remove_lock_file(&admin_dir.join("index.lock"))?;
            }
        }

        info!(
            stage = "git",
            event = "git.clear_stale_index_locks.end",
            repo = %repo.name,
            removed_lock_count = removed,
            mirror = %paths.mirror.display(),
            "cleared stale git index locks"
        );

        Ok(removed)
    }

    pub async fn fetch_branches(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
        branches: &[String],
    ) -> Result<()> {
        self.fetch_exact_branches(paths, branches, "fetch_branches", Some(repo.name.as_str()))
            .await
            .with_context(|| format!("git fetch failed for repo '{}'", repo.name))?;
        Ok(())
    }

    async fn fetch_exact_branches(
        &self,
        paths: &RepoPaths,
        branches: &[String],
        operation: &str,
        repo: Option<&str>,
    ) -> Result<()> {
        if branches.is_empty() {
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

        for branch in branches {
            let refspec = format!("+refs/heads/{0}:refs/remotes/origin/{0}", branch.trim());
            args.push(refspec);
        }

        self.run(args, None, operation, repo, None).await
    }

    pub async fn resolve_branches(
        &self,
        repo: &RepoConfig,
        paths: &RepoPaths,
    ) -> Result<BTreeMap<String, String>> {
        let remote_heads = self.list_origin_heads(paths, repo).await?;
        let heads = select_branches(repo, &remote_heads)?;
        let branches = heads.keys().cloned().collect::<Vec<_>>();

        self.fetch_branches(repo, paths, &branches).await?;

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

    async fn list_origin_heads(
        &self,
        paths: &RepoPaths,
        repo: &RepoConfig,
    ) -> Result<BTreeMap<String, String>> {
        let mirror = paths.mirror.display().to_string();
        let output = self
            .run_capture(
                [
                    "--git-dir",
                    mirror.as_str(),
                    "ls-remote",
                    "--heads",
                    "origin",
                ],
                None,
                "list_origin_heads",
                Some(repo.name.as_str()),
                None,
            )
            .await?;

        let mut branches = BTreeMap::new();
        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let Some((sha, refname)) = line.split_once('\t') else {
                bail!(
                    "repo '{}' returned malformed ls-remote line '{}'",
                    repo.name,
                    line
                );
            };
            let Some(branch) = refname.strip_prefix("refs/heads/") else {
                continue;
            };

            branches.insert(branch.to_string(), sha.to_string());
        }

        Ok(branches)
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

fn select_branches(
    repo: &RepoConfig,
    remote_heads: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    if remote_heads.is_empty() {
        bail!("repo '{}' has no remote branches", repo.name);
    }

    let mut wanted = BTreeSet::new();

    for branch in &repo.branches {
        if remote_heads.contains_key(branch) {
            wanted.insert(branch.clone());
        }
    }

    for configured in &repo.branch_patterns {
        let pattern = Pattern::new(configured).with_context(|| {
            format!(
                "repo '{}' has invalid branch pattern '{}'",
                repo.name, configured
            )
        })?;
        for branch in remote_heads.keys() {
            if pattern.matches(branch) {
                wanted.insert(branch.clone());
            }
        }
    }

    if wanted.is_empty() {
        bail!(
            "repo '{}' branches {:?} and branch_patterns {:?} matched no remote branches",
            repo.name,
            repo.branches,
            repo.branch_patterns
        );
    }

    Ok(wanted
        .into_iter()
        .filter_map(|branch| remote_heads.get(&branch).map(|sha| (branch, sha.clone())))
        .collect())
}

fn remove_lock_file(path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }

    fs::remove_file(path)
        .with_context(|| format!("failed to remove stale git lock {}", path.display()))?;
    Ok(1)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_config(branches: Vec<&str>, branch_patterns: Vec<&str>) -> RepoConfig {
        RepoConfig {
            name: "pointer".to_string(),
            url: "git@example.com:pointer.git".to_string(),
            interval: std::time::Duration::from_secs(60),
            branches: branches.into_iter().map(str::to_string).collect(),
            branch_patterns: branch_patterns.into_iter().map(str::to_string).collect(),
            indexer_args: Vec::new(),
            per_branch: Vec::new(),
            pre_index_hooks: Vec::new(),
            post_upload_hooks: Vec::new(),
        }
    }

    #[test]
    fn select_branches_unions_exact_and_pattern_matches() {
        let repo = repo_config(vec!["main"], vec!["rc-*", "release/*"]);
        let remote_heads = BTreeMap::from([
            ("feature/foo".to_string(), "ddd".to_string()),
            ("main".to_string(), "aaa".to_string()),
            ("rc-1".to_string(), "bbb".to_string()),
            ("release/1.0".to_string(), "ccc".to_string()),
        ]);

        let heads = select_branches(&repo, &remote_heads).expect("select branches");

        assert_eq!(
            heads,
            BTreeMap::from([
                ("main".to_string(), "aaa".to_string()),
                ("rc-1".to_string(), "bbb".to_string()),
                ("release/1.0".to_string(), "ccc".to_string()),
            ])
        );
    }

    #[test]
    fn select_branches_errors_when_nothing_matches() {
        let repo = repo_config(vec!["main"], vec!["rc-*"]);
        let remote_heads = BTreeMap::from([("develop".to_string(), "aaa".to_string())]);

        let err = select_branches(&repo, &remote_heads).expect_err("should fail");

        assert!(err.to_string().contains("matched no remote branches"));
    }

    #[test]
    fn clears_stale_index_locks_in_mirror_and_worktrees() {
        let temp = std::env::temp_dir().join(format!(
            "pointer-reposerver-git-lock-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp);
        fs::create_dir_all(temp.join("mirror.git/worktrees/feature"))
            .expect("create admin worktree dir");
        fs::write(temp.join("mirror.git/index.lock"), "").expect("write mirror lock");
        fs::write(temp.join("mirror.git/worktrees/feature/index.lock"), "")
            .expect("write worktree lock");

        let git = Git::new("git");
        let repo = repo_config(vec!["main"], Vec::new());
        let paths = RepoPaths {
            mirror: temp.join("mirror.git"),
            worktrees_root: temp.join("worktrees"),
        };

        let removed = git
            .clear_stale_index_locks(&repo, &paths)
            .expect("clear locks");

        assert_eq!(removed, 2);
        assert!(!paths.mirror.join("index.lock").exists());
        assert!(!paths.mirror.join("worktrees/feature/index.lock").exists());

        fs::remove_dir_all(&temp).expect("remove temp dir");
    }
}
