use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use glob::Pattern;
use humantime::parse_duration;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub global: GlobalConfig,
    pub repos: Vec<RepoConfig>,
}

#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub state_dir: PathBuf,
    pub default_interval: Duration,
    pub max_repo_concurrency: usize,
    pub git_bin: String,
    pub indexer_bin: String,
    pub indexer_args: Vec<String>,
    pub finish_hook: Option<HookConfig>,
}

#[derive(Debug, Clone)]
pub struct RepoConfig {
    pub name: String,
    pub url: String,
    pub interval: Duration,
    pub branches: Vec<String>,
    pub indexer_args: Vec<String>,
    pub per_branch: Vec<PerBranchConfig>,
    pub pre_index_hooks: Vec<HookConfig>,
    pub post_upload_hooks: Vec<HookConfig>,
}

#[derive(Debug, Clone)]
pub struct PerBranchConfig {
    pub branch: String,
    pub indexer_args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct HookConfig {
    pub command: String,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    #[serde(default)]
    global: RawGlobalConfig,
    #[serde(rename = "repo", default)]
    repos: Vec<RawRepoConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RawGlobalConfig {
    state_dir: Option<PathBuf>,
    default_interval: Option<String>,
    max_repo_concurrency: Option<usize>,
    git_bin: Option<String>,
    indexer_bin: Option<String>,
    #[serde(default)]
    indexer_args: Vec<String>,
    finish_hook: Option<RawHookConfig>,
}

#[derive(Debug, Deserialize)]
struct RawRepoConfig {
    name: String,
    url: String,
    interval: Option<String>,
    branches: Vec<String>,
    #[serde(default)]
    indexer_args: Vec<String>,
    #[serde(default)]
    per_branch: Vec<RawPerBranchConfig>,
    #[serde(default)]
    pre_index_hooks: Vec<RawHookConfig>,
    #[serde(default)]
    post_upload_hooks: Vec<RawHookConfig>,
}

#[derive(Debug, Deserialize)]
struct RawPerBranchConfig {
    branch: String,
    #[serde(default)]
    indexer_args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawHookConfig {
    command: String,
    timeout: Option<String>,
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;

        let parsed: FileConfig = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML {}", path.display()))?;

        Self::from_raw(parsed)
    }

    fn from_raw(raw: FileConfig) -> Result<Self> {
        if raw.repos.is_empty() {
            bail!("config must include at least one [[repo]] entry");
        }

        let state_dir = raw
            .global
            .state_dir
            .unwrap_or_else(|| PathBuf::from(".reposerver-state"));

        let default_interval = parse_duration_string(
            raw.global.default_interval.as_deref().unwrap_or("5m"),
            "global.default_interval",
        )?;

        let max_repo_concurrency = raw.global.max_repo_concurrency.unwrap_or(1).max(1);

        let git_bin = raw.global.git_bin.unwrap_or_else(|| "git".to_string());
        let indexer_bin = raw
            .global
            .indexer_bin
            .unwrap_or_else(|| "pointer-indexer".to_string());

        if git_bin.trim().is_empty() {
            bail!("global.git_bin must not be empty");
        }
        if indexer_bin.trim().is_empty() {
            bail!("global.indexer_bin must not be empty");
        }

        let global = GlobalConfig {
            state_dir,
            default_interval,
            max_repo_concurrency,
            git_bin,
            indexer_bin,
            indexer_args: raw.global.indexer_args,
            finish_hook: raw
                .global
                .finish_hook
                .map(|hook| build_hook(hook, "global.finish_hook"))
                .transpose()?,
        };

        let mut repos = Vec::with_capacity(raw.repos.len());
        for repo in raw.repos {
            repos.push(build_repo(repo, global.default_interval)?);
        }

        Ok(Self { global, repos })
    }

    pub fn validate_config(&self) -> Result<()> {
        if self.repos.is_empty() {
            bail!("config must include at least one [[repo]] entry");
        }

        if let Some(hook) = &self.global.finish_hook {
            if hook.command.trim().is_empty() {
                bail!("global.finish_hook.command must not be empty");
            }
        }

        for repo in &self.repos {
            if repo.name.trim().is_empty() {
                bail!("repo.name must not be empty");
            }
            if repo.url.trim().is_empty() {
                bail!("repo.url must not be empty for repo '{}'", repo.name);
            }
            if repo.branches.is_empty() {
                bail!(
                    "repo '{}' must define at least one branch pattern",
                    repo.name
                );
            }

            for pattern in &repo.branches {
                if pattern.trim().is_empty() {
                    bail!("repo '{}' contains an empty branch pattern", repo.name);
                }
                if is_glob_pattern(pattern) {
                    Pattern::new(pattern).with_context(|| {
                        format!("repo '{}' has invalid branch glob '{}'", repo.name, pattern)
                    })?;
                }
            }

            for hook in repo
                .pre_index_hooks
                .iter()
                .chain(repo.post_upload_hooks.iter())
            {
                if hook.command.trim().is_empty() {
                    bail!("repo '{}' has a hook with empty command", repo.name);
                }
            }

            let mut seen = HashSet::new();
            for cfg in &repo.per_branch {
                if cfg.branch.trim().is_empty() {
                    bail!(
                        "repo '{}' has a per_branch entry with an empty branch",
                        repo.name
                    );
                }
                if is_glob_pattern(&cfg.branch) {
                    bail!(
                        "repo '{}' per_branch.branch must be an exact branch name, got '{}'",
                        repo.name,
                        cfg.branch
                    );
                }
                if !seen.insert(cfg.branch.as_str()) {
                    bail!(
                        "repo '{}' has duplicate per_branch config for branch '{}'",
                        repo.name,
                        cfg.branch
                    );
                }
            }
        }

        Ok(())
    }
}

fn build_repo(raw: RawRepoConfig, default_interval: Duration) -> Result<RepoConfig> {
    let interval = if let Some(raw_interval) = raw.interval.as_deref() {
        parse_duration_string(raw_interval, &format!("repo '{}'.interval", raw.name))?
    } else {
        default_interval
    };

    let pre_index_hooks = raw
        .pre_index_hooks
        .into_iter()
        .map(|hook| build_hook(hook, &format!("repo '{}'.pre_index_hooks", raw.name)))
        .collect::<Result<Vec<_>>>()?;

    let post_upload_hooks = raw
        .post_upload_hooks
        .into_iter()
        .map(|hook| build_hook(hook, &format!("repo '{}'.post_upload_hooks", raw.name)))
        .collect::<Result<Vec<_>>>()?;

    let per_branch = raw
        .per_branch
        .into_iter()
        .map(|cfg| PerBranchConfig {
            branch: cfg.branch,
            indexer_args: cfg.indexer_args,
        })
        .collect::<Vec<_>>();

    let mut branches = raw.branches;
    for cfg in &per_branch {
        if !branches.iter().any(|b| b == &cfg.branch) {
            branches.push(cfg.branch.clone());
        }
    }

    Ok(RepoConfig {
        name: raw.name,
        url: raw.url,
        interval,
        branches,
        indexer_args: raw.indexer_args,
        per_branch,
        pre_index_hooks,
        post_upload_hooks,
    })
}

fn build_hook(raw: RawHookConfig, context: &str) -> Result<HookConfig> {
    let timeout = if let Some(timeout) = raw.timeout.as_deref() {
        Some(parse_duration_string(
            timeout,
            &format!("{context}.timeout"),
        )?)
    } else {
        None
    };

    Ok(HookConfig {
        command: raw.command,
        timeout,
    })
}

fn parse_duration_string(value: &str, field: &str) -> Result<Duration> {
    let duration = parse_duration(value)
        .with_context(|| format!("invalid duration for {field}: '{value}'"))?;

    if duration.is_zero() {
        return Err(anyhow!("duration for {field} must be greater than zero"));
    }

    Ok(duration)
}

fn is_glob_pattern(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_config() {
        let raw = r#"
            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let cfg = AppConfig::from_raw(parsed).expect("normalize");

        assert_eq!(cfg.repos.len(), 1);
        assert_eq!(cfg.repos[0].interval, Duration::from_secs(300));
        assert_eq!(cfg.global.max_repo_concurrency, 1);
        assert!(cfg.global.indexer_args.is_empty());
    }

    #[test]
    fn rejects_zero_duration() {
        let raw = r#"
            [global]
            default_interval = "0s"

            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let err = AppConfig::from_raw(parsed).expect_err("should fail");
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn parses_global_indexer_args() {
        let raw = r#"
            [global]
            indexer_args = ["--upload-url", "http://localhost:8080/api/v1/index"]

            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
            indexer_args = ["--keep-latest", "3"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let cfg = AppConfig::from_raw(parsed).expect("normalize");

        assert_eq!(
            cfg.global.indexer_args,
            vec![
                "--upload-url".to_string(),
                "http://localhost:8080/api/v1/index".to_string()
            ]
        );
        assert_eq!(
            cfg.repos[0].indexer_args,
            vec!["--keep-latest".to_string(), "3".to_string()]
        );
    }

    #[test]
    fn parses_global_finish_hook() {
        let raw = r#"
            [global.finish_hook]
            command = "echo done"
            timeout = "10s"

            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let cfg = AppConfig::from_raw(parsed).expect("normalize");
        let hook = cfg.global.finish_hook.expect("finish hook");
        assert_eq!(hook.command, "echo done");
        assert_eq!(hook.timeout.expect("timeout"), Duration::from_secs(10));
    }

    #[test]
    fn rejects_empty_global_finish_hook_command() {
        let raw = r#"
            [global.finish_hook]
            command = "   "

            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let cfg = AppConfig::from_raw(parsed).expect("normalize");
        let err = cfg.validate_config().expect_err("should fail");
        assert!(err.to_string().contains("global.finish_hook.command"));
    }

    #[test]
    fn rejects_zero_global_finish_hook_timeout() {
        let raw = r#"
            [global.finish_hook]
            command = "echo done"
            timeout = "0s"

            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]
        "#;
        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let err = AppConfig::from_raw(parsed).expect_err("should fail");
        assert!(err.to_string().contains("greater than zero"));
    }

    #[test]
    fn parses_per_branch_indexer_args_and_merges_branches() {
        let raw = r#"
            [[repo]]
            name = "foo"
            url = "git@example.com:foo.git"
            branches = ["main"]

            [[repo.per_branch]]
            branch = "release"
            indexer_args = ["--live"]
        "#;

        let parsed: FileConfig = toml::from_str(raw).expect("parse config");
        let cfg = AppConfig::from_raw(parsed).expect("normalize");

        assert_eq!(
            cfg.repos[0].branches,
            vec!["main".to_string(), "release".to_string()]
        );
        assert_eq!(cfg.repos[0].per_branch.len(), 1);
        assert_eq!(cfg.repos[0].per_branch[0].branch, "release");
        assert_eq!(
            cfg.repos[0].per_branch[0].indexer_args,
            vec!["--live".to_string()]
        );
    }
}
