use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use git2::Repository;
use sha2::{Digest, Sha256};
use tracing::warn;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Clone)]
pub struct RepoMetadata {
    pub commit: String,
    pub branch: Option<String>,
}

pub fn init_tracing(verbosity: u8) -> Result<()> {
    let default_directive = match verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_directive));

    let subscriber = fmt().with_env_filter(filter).with_target(true).finish();

    if tracing::subscriber::set_global_default(subscriber).is_err() {
        warn!("tracing subscriber already initialized");
    }

    Ok(())
}

pub fn default_repo_name(path: &Path) -> String {
    path.file_name()
        .and_then(|os| os.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "workspace".into())
}

pub fn resolve_repo_metadata(
    repo_path: &Path,
    commit: Option<String>,
    branch: Option<String>,
) -> Result<RepoMetadata> {
    let repo = Repository::discover(repo_path)
        .with_context(|| format!("failed to open git repository at {}", repo_path.display()))?;

    let resolved_commit = match commit {
        Some(c) => c,
        None => repo
            .head()
            .and_then(|head| head.peel_to_commit())
            .map(|commit| commit.id().to_string())
            .with_context(|| {
                format!("could not resolve HEAD commit for {}", repo_path.display())
            })?,
    };

    let resolved_branch = match branch {
        Some(b) => Some(b),
        None => repo.head().ok().and_then(|head| {
            if head.is_branch() {
                head.shorthand().map(|s| s.to_string())
            } else {
                None
            }
        }),
    };

    Ok(RepoMetadata {
        commit: resolved_commit,
        branch: resolved_branch,
    })
}

pub fn infer_language(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|s| s.to_ascii_lowercase())
    {
        Some(ref ext) if ext == "rs" => Some("rust"),
        Some(ref ext) if matches!(ext.as_str(), "ts" | "tsx") => Some("typescript"),
        Some(ref ext) if matches!(ext.as_str(), "js" | "jsx") => Some("javascript"),
        Some(ref ext) if ext == "py" => Some("python"),
        Some(ref ext) if ext == "go" => Some("go"),
        Some(ref ext) if matches!(ext.as_str(), "java" | "kt") => Some("jvm"),
        Some(ref ext) if matches!(ext.as_str(), "c") => Some("c"),
        Some(ref ext) if matches!(ext.as_str(), "m" | "mm") => Some("objc"),
        Some(ref ext)
            if matches!(
                ext.as_str(),
                "cc" | "inl" | "cpp" | "cxx" | "hpp" | "hh" | "h"
            ) =>
        {
            Some("cpp")
        }
        Some(ref ext) if ext == "nix" => Some("nix"),
        Some(ref ext) if ext == "proto" => Some("proto"),
        Some(ref ext) if ext == "swift" => Some("swift"),
        _ => None,
    }
}

pub fn compute_content_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn line_count(bytes: &[u8]) -> i32 {
    if bytes.is_empty() {
        return 0;
    }

    let line_breaks = bytes.iter().filter(|b| **b == b'\n').count();
    (line_breaks + 1) as i32
}

pub fn normalize_relative_path(path: &Path) -> String {
    path.iter()
        .map(|component| component.to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

pub fn namespace_from_path(language: Option<&str>, relative_path: &Path) -> Option<String> {
    match language {
        Some("rust") => rust_namespace(relative_path),
        _ => default_namespace(relative_path),
    }
}

fn rust_namespace(relative_path: &Path) -> Option<String> {
    let mut components: Vec<String> = relative_path
        .iter()
        .filter_map(|c| c.to_str())
        .map(|s| s.to_string())
        .collect();

    if components.is_empty() {
        return None;
    }

    if components.first().map(|c| c == "src").unwrap_or(false) {
        components.remove(0);
    }

    if components.is_empty() {
        return Some("crate".to_string());
    }

    let file = components.pop().unwrap();

    match file.as_str() {
        "lib.rs" => {
            if components.is_empty() {
                Some("crate".into())
            } else {
                Some(components.join("::"))
            }
        }
        "mod.rs" => {
            if components.is_empty() {
                Some("crate".into())
            } else {
                Some(components.join("::"))
            }
        }
        name => {
            let module = name.trim_end_matches(".rs");
            components.push(module.to_string());
            Some(components.join("::"))
        }
    }
}

fn default_namespace(relative_path: &Path) -> Option<String> {
    let mut components: Vec<String> = relative_path
        .iter()
        .filter_map(|c| c.to_str())
        .map(|s| s.to_string())
        .collect();

    if components.is_empty() {
        return None;
    }

    if components.first().map(|c| c == "src").unwrap_or(false) {
        components.remove(0);
    }

    if components.is_empty() {
        return None;
    }

    let file = components.pop().unwrap();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file);

    components.push(stem.to_string());

    Some(components.join("::"))
}

pub fn ensure_relative(path: &Path, root: &Path) -> Result<PathBuf> {
    path.strip_prefix(root)
        .map(|p| p.to_path_buf())
        .with_context(|| format!("{} is not inside {}", path.display(), root.display()))
}
