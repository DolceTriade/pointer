use anyhow::{Context, Result, anyhow};
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::cli::{
    AdminArgs, AdminCommand, CleanupSymbolCacheArgs, PruneBranchArgs, PruneCommitArgs, PruneRepoArgs,
    PrunePolicyArgs, RefreshSymbolCacheArgs,
};

const REQUEST_TIMEOUT_SECS: u64 = 3600;

pub fn run_admin(args: AdminArgs) -> Result<()> {
    let base_url = args
        .backend_url
        .as_deref()
        .ok_or_else(|| anyhow!("--backend-url or POINTER_BACKEND_URL is required"))?;

    let endpoints = AdminEndpoints::new(base_url);
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .build()
        .context("failed to build HTTP client")?;

    match args.command {
        AdminCommand::Gc => run_gc(&client, &endpoints, args.api_key.as_deref()),
        AdminCommand::RebuildSymbolCache => {
            rebuild_symbol_cache(&client, &endpoints, args.api_key.as_deref())
        }
        AdminCommand::CleanupSymbolCache(payload) => {
            cleanup_symbol_cache(&client, &endpoints, args.api_key.as_deref(), payload)
        }
        AdminCommand::RefreshSymbolCache(payload) => {
            refresh_symbol_cache(&client, &endpoints, args.api_key.as_deref(), payload)
        }
        AdminCommand::PruneCommit(payload) => {
            prune_commit(&client, &endpoints, args.api_key.as_deref(), payload)
        }
        AdminCommand::PruneBranch(payload) => {
            prune_branch(&client, &endpoints, args.api_key.as_deref(), payload)
        }
        AdminCommand::PruneRepo(payload) => {
            prune_repo(&client, &endpoints, args.api_key.as_deref(), payload)
        }
        AdminCommand::PrunePolicy(payload) => {
            prune_policy(&client, &endpoints, args.api_key.as_deref(), payload)
        }
    }
}

#[derive(Clone)]
struct AdminEndpoints {
    gc: String,
    rebuild_symbol_cache: String,
    cleanup_symbol_cache: String,
    refresh_symbol_cache: String,
    prune_commit: String,
    prune_branch: String,
    prune_repo: String,
    prune_policy: String,
}

impl AdminEndpoints {
    fn new(base: &str) -> Self {
        let trimmed = base.trim_end_matches('/');
        Self {
            gc: format!("{}/admin/gc", trimmed),
            rebuild_symbol_cache: format!("{}/admin/rebuild_symbol_cache", trimmed),
            cleanup_symbol_cache: format!("{}/admin/cleanup_symbol_cache", trimmed),
            refresh_symbol_cache: format!("{}/admin/refresh_symbol_cache", trimmed),
            prune_commit: format!("{}/prune/commit", trimmed),
            prune_branch: format!("{}/prune/branch", trimmed),
            prune_repo: format!("{}/prune/repo", trimmed),
            prune_policy: format!("{}/prune/policy", trimmed),
        }
    }
}

#[derive(Debug, Deserialize)]
struct GcResponse {
    branches_evaluated: usize,
    snapshots_removed: usize,
    commits_pruned: usize,
}

fn run_gc(client: &Client, endpoints: &AdminEndpoints, api_key: Option<&str>) -> Result<()> {
    let response: GcResponse = post_json(client, &endpoints.gc, api_key, &())?
        .json()
        .context("failed to deserialize gc response")?;
    info!(
        branches = response.branches_evaluated,
        snapshots_removed = response.snapshots_removed,
        commits_pruned = response.commits_pruned,
        "gc completed"
    );
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RebuildSymbolCacheResponse {
    message: String,
    shard_count: usize,
    inserted_names: u64,
    inserted_refs: u64,
}

fn rebuild_symbol_cache(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
) -> Result<()> {
    let response: RebuildSymbolCacheResponse = post_json(
        client,
        &endpoints.rebuild_symbol_cache,
        api_key,
        &(),
    )?
    .json()
    .context("failed to deserialize rebuild response")?;

    info!(
        shard_count = response.shard_count,
        inserted_names = response.inserted_names,
        inserted_refs = response.inserted_refs,
        message = response.message,
        "symbol cache rebuilt"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct CleanupSymbolCacheRequest {
    batch_size: i64,
    max_batches: i64,
}

#[derive(Debug, Deserialize)]
struct CleanupSymbolCacheResponse {
    refs_deleted: i64,
    names_deleted: i64,
    batches_run: i64,
}

fn cleanup_symbol_cache(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: CleanupSymbolCacheArgs,
) -> Result<()> {
    let request = CleanupSymbolCacheRequest {
        batch_size: payload.batch_size,
        max_batches: payload.max_batches,
    };
    let response: CleanupSymbolCacheResponse = post_json(
        client,
        &endpoints.cleanup_symbol_cache,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize cleanup response")?;

    info!(
        refs_deleted = response.refs_deleted,
        names_deleted = response.names_deleted,
        batches_run = response.batches_run,
        "symbol cache cleanup complete"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct RefreshSymbolCacheRequest {
    batch_size: i64,
    max_batches: i64,
}

#[derive(Debug, Deserialize)]
struct RefreshSymbolCacheResponse {
    names_inserted: i64,
    batches_run: i64,
}

fn refresh_symbol_cache(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: RefreshSymbolCacheArgs,
) -> Result<()> {
    let request = RefreshSymbolCacheRequest {
        batch_size: payload.batch_size,
        max_batches: payload.max_batches,
    };
    let response: RefreshSymbolCacheResponse = post_json(
        client,
        &endpoints.refresh_symbol_cache,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize refresh response")?;

    info!(
        names_inserted = response.names_inserted,
        batches_run = response.batches_run,
        "symbol cache refresh complete"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct PruneCommitRequest {
    repository: String,
    commit_sha: String,
}

#[derive(Debug, Deserialize)]
struct PruneCommitResponse {
    repository: String,
    commit_sha: String,
    pruned: bool,
    message: String,
}

fn prune_commit(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: PruneCommitArgs,
) -> Result<()> {
    let request = PruneCommitRequest {
        repository: payload.repository,
        commit_sha: payload.commit_sha,
    };
    let response: PruneCommitResponse = post_json(
        client,
        &endpoints.prune_commit,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize prune commit response")?;

    info!(
        repository = response.repository,
        commit = response.commit_sha,
        pruned = response.pruned,
        message = response.message,
        "commit pruning complete"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct PruneBranchRequest {
    repository: String,
    branch: String,
}

#[derive(Debug, Deserialize)]
struct PruneBranchResponse {
    repository: String,
    branch: String,
    pruned: bool,
    message: String,
}

fn prune_branch(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: PruneBranchArgs,
) -> Result<()> {
    let request = PruneBranchRequest {
        repository: payload.repository,
        branch: payload.branch,
    };
    let response: PruneBranchResponse = post_json(
        client,
        &endpoints.prune_branch,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize prune branch response")?;

    info!(
        repository = response.repository,
        branch = response.branch,
        pruned = response.pruned,
        message = response.message,
        "branch pruning complete"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct PruneRepoRequest {
    repository: String,
    batch_size: i64,
}

#[derive(Debug, Deserialize)]
struct PruneRepoResponse {
    repository: String,
    pruned: bool,
    deleted_rows: i64,
    message: String,
}

fn prune_repo(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: PruneRepoArgs,
) -> Result<()> {
    let request = PruneRepoRequest {
        repository: payload.repository,
        batch_size: payload.batch_size,
    };
    let response: PruneRepoResponse = post_json(
        client,
        &endpoints.prune_repo,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize prune repo response")?;

    info!(
        repository = response.repository,
        pruned = response.pruned,
        deleted_rows = response.deleted_rows,
        message = response.message,
        "repository pruning complete"
    );
    Ok(())
}

#[derive(Debug, Serialize)]
struct PrunePolicyRequest {
    repository: String,
    keep_latest: bool,
    max_commits_to_keep: Option<i32>,
}

#[derive(Debug, Deserialize)]
struct PrunePolicyResponse {
    repository: String,
    message: String,
}

fn prune_policy(
    client: &Client,
    endpoints: &AdminEndpoints,
    api_key: Option<&str>,
    payload: PrunePolicyArgs,
) -> Result<()> {
    let request = PrunePolicyRequest {
        repository: payload.repository,
        keep_latest: payload.keep_latest,
        max_commits_to_keep: payload.max_commits_to_keep,
    };
    let response: PrunePolicyResponse = post_json(
        client,
        &endpoints.prune_policy,
        api_key,
        &request,
    )?
    .json()
    .context("failed to deserialize prune policy response")?;

    info!(
        repository = response.repository,
        message = response.message,
        "retention policy applied"
    );
    Ok(())
}

fn post_json<T: Serialize>(
    client: &Client,
    url: &str,
    api_key: Option<&str>,
    body: &T,
) -> Result<Response> {
    let mut request = client
        .post(url)
        .header(CONTENT_TYPE, "application/json")
        .json(body);

    if let Some(key) = api_key {
        request = request.header(AUTHORIZATION, format!("Bearer {}", key));
    }

    let response = request
        .send()
        .with_context(|| format!("failed request to {}", url))?;
    if !response.status().is_success() {
        let status = response.status();
        let message = response.text().unwrap_or_default();
        anyhow::bail!("request to {url} failed with status {status}: {message}");
    }

    Ok(response)
}
