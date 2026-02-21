use axum::Json;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::db::models::{FacetCount, SearchResult, SearchResultsPage};
use crate::db::{Database, postgres::PostgresDb};
use crate::mcp::types::{
    ApiResponse, BranchFreshness, FileContentToolRequest, FileContentToolResponse, FileListEntry,
    FileListToolRequest, FileListToolResponse, IndexFreshness, PathSearchToolRequest,
    PathSearchToolResponse, RepoBranchesToolRequest, RepoBranchesToolResponse,
    RepositoriesToolRequest, RepositoriesToolResponse, SearchDedupeMode, SearchToolRequest,
    SymbolInsightsToolRequest, SymbolInsightsToolResponse,
};
use crate::pages::file_viewer::{fetch_symbol_insights, search_repo_paths};
use crate::pages::repo_detail::{RepoBranchDisplay, get_repo_branches};
use crate::services::repo_service::get_repositories;
use crate::services::search_service::search;

const MAX_BATCH_QUERIES: usize = 8;

pub async fn execute_search(payload: SearchToolRequest) -> Result<Value, String> {
    let mode = validate_search_mode(payload)?;
    match mode {
        SearchExecutionMode::Single { query, page } => execute_single_search(query, page).await,
        SearchExecutionMode::Batch {
            queries,
            dedupe,
            max_results_per_query,
        } => execute_batch_search(queries, dedupe, max_results_per_query).await,
    }
}

pub async fn execute_repo_branches(
    payload: RepoBranchesToolRequest,
) -> Result<RepoBranchesToolResponse, String> {
    let branches = get_repo_branches(payload.repo)
        .await
        .map_err(|err| err.to_string())?;
    let index_freshness = branches
        .iter()
        .map(|branch| BranchFreshness {
            name: branch.name.clone(),
            indexed_at: branch.indexed_at.clone(),
            age_seconds: branch
                .indexed_at
                .as_deref()
                .and_then(age_seconds_from_rfc3339),
            age_human: branch
                .indexed_at
                .as_deref()
                .and_then(age_seconds_from_rfc3339)
                .map(format_age_human),
        })
        .collect();
    Ok(RepoBranchesToolResponse {
        branches,
        index_freshness,
    })
}

pub async fn execute_file_content(
    payload: FileContentToolRequest,
) -> Result<FileContentToolResponse, String> {
    let state = leptos::prelude::expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let commit = db
        .resolve_branch_head(&payload.repo, &payload.branch)
        .await
        .map_err(|err| err.to_string())?
        .unwrap_or_else(|| payload.branch.clone());

    let raw = db
        .get_file_content(&payload.repo, &commit, &payload.path)
        .await
        .map_err(|err| err.to_string())?;

    let line_count = raw.content.lines().count();
    let index_freshness = resolve_branch_freshness(&payload.repo, &payload.branch, Some(&commit))
        .await
        .unwrap_or_else(|_| unknown_freshness());

    Ok(FileContentToolResponse {
        repository: raw.repository,
        commit_sha: raw.commit_sha,
        file_path: raw.file_path,
        language: raw.language,
        content: raw.content,
        line_count,
        index_freshness,
    })
}

pub async fn execute_file_list(
    payload: FileListToolRequest,
) -> Result<FileListToolResponse, String> {
    let state = leptos::prelude::expect_context::<crate::server::GlobalAppState>();
    let state = state.lock().await;
    let db = PostgresDb::new(state.pool.clone());

    let commit = db
        .resolve_branch_head(&payload.repo, &payload.branch)
        .await
        .map_err(|err| err.to_string())?
        .unwrap_or_else(|| payload.branch.clone());

    let root_path = payload
        .path
        .unwrap_or_default()
        .trim_matches('/')
        .to_string();
    let requested_depth = payload.depth.clamp(1, 10);
    let limit = payload.limit.clamp(1, 5000);

    let mut queue: VecDeque<(String, u8)> = VecDeque::new();
    queue.push_back((root_path.clone(), 0));
    let mut visited_dirs = HashSet::new();
    visited_dirs.insert(root_path.clone());

    let mut entries: Vec<FileListEntry> = Vec::new();
    let mut truncated = false;

    while let Some((dir_path, dir_depth)) = queue.pop_front() {
        let tree = db
            .get_repo_tree(
                &payload.repo,
                crate::db::RepoTreeQuery {
                    commit: commit.clone(),
                    path: Some(dir_path.clone()),
                },
            )
            .await
            .map_err(|err| err.to_string())?;

        let child_depth = dir_depth.saturating_add(1);
        for entry in tree.entries {
            entries.push(FileListEntry {
                name: entry.name.clone(),
                path: entry.path.clone(),
                kind: entry.kind.clone(),
                depth: child_depth,
            });

            if entries.len() >= limit {
                truncated = true;
                break;
            }

            if entry.kind == "dir" && child_depth < requested_depth {
                if visited_dirs.insert(entry.path.clone()) {
                    queue.push_back((entry.path, child_depth));
                }
            }
        }

        if truncated {
            break;
        }
    }

    entries.sort_by(|a, b| match (a.kind.as_str(), b.kind.as_str()) {
        ("dir", "file") => std::cmp::Ordering::Less,
        ("file", "dir") => std::cmp::Ordering::Greater,
        _ => a.path.cmp(&b.path),
    });

    let index_freshness = resolve_branch_freshness(&payload.repo, &payload.branch, Some(&commit))
        .await
        .unwrap_or_else(|_| unknown_freshness());

    Ok(FileListToolResponse {
        repository: payload.repo,
        commit_sha: commit,
        root_path,
        requested_depth,
        truncated,
        entries,
        index_freshness,
    })
}

pub async fn execute_path_search(
    payload: PathSearchToolRequest,
) -> Result<PathSearchToolResponse, String> {
    if payload.query.trim().is_empty() {
        return Err("query must be non-empty for path_search".to_string());
    }
    let repo = payload.repo.clone();
    let branch = payload.branch.clone();
    let entries = search_repo_paths(repo.clone(), branch.clone(), payload.query, payload.limit)
        .await
        .map_err(|err| err.to_string())?;
    let index_freshness = resolve_branch_freshness(&repo, &branch, None)
        .await
        .unwrap_or_else(|_| unknown_freshness());
    Ok(PathSearchToolResponse {
        entries,
        index_freshness,
    })
}

pub async fn execute_symbol_insights(
    payload: SymbolInsightsToolRequest,
) -> Result<SymbolInsightsToolResponse, String> {
    let repo = payload.params.repo.clone();
    let branch = payload.params.branch.clone();
    let insights = fetch_symbol_insights(payload.params)
        .await
        .map_err(|err| err.to_string())?;
    let index_freshness = resolve_branch_freshness(&repo, &branch, None)
        .await
        .unwrap_or_else(|_| unknown_freshness());
    Ok(SymbolInsightsToolResponse {
        insights,
        index_freshness,
    })
}

pub async fn execute_repositories(
    payload: RepositoriesToolRequest,
) -> Result<RepositoriesToolResponse, String> {
    let repositories = get_repositories(payload.limit)
        .await
        .map_err(|err| err.to_string())?;
    Ok(RepositoriesToolResponse {
        repositories,
        index_freshness: unknown_freshness(),
    })
}

pub fn normalize_tool_error(tool: &str, err: String) -> (String, String, Option<String>) {
    let lower = err.to_ascii_lowercase();

    if lower.contains("params must not have additional properties")
        || lower.contains("unknown field")
        || lower.contains("invalid type")
    {
        return (
            format!("{tool}_invalid_params"),
            "request does not match tool input schema".to_string(),
            Some("Call tools/list and send only the documented arguments.".to_string()),
        );
    }
    if lower.contains("repository") && lower.contains("not found") {
        return (
            format!("{tool}_repository_not_found"),
            err,
            Some("Call repositories first to get the exact indexed repository key.".to_string()),
        );
    }
    if lower.contains("branch") && lower.contains("not found") {
        return (
            format!("{tool}_branch_not_found"),
            err,
            Some(
                "Call repo_branches for the repository and retry with an exact branch name."
                    .to_string(),
            ),
        );
    }
    if tool == "path_search" && lower.contains("non-empty") {
        return (
            "path_search_empty_query".to_string(),
            err,
            Some(
                "Use path_search with a non-empty query, or file_list to enumerate directories."
                    .to_string(),
            ),
        );
    }

    (
        format!("{tool}_failed"),
        err,
        Some(
            "If this persists, call tools/list and retry with a minimal valid payload.".to_string(),
        ),
    )
}

pub async fn tool_search(
    Json(payload): Json<SearchToolRequest>,
) -> Json<ApiResponse<serde_json::Value>> {
    match execute_search(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("search", err);
            Json(ApiResponse::<serde_json::Value>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_repo_branches(
    Json(payload): Json<RepoBranchesToolRequest>,
) -> Json<ApiResponse<RepoBranchesToolResponse>> {
    match execute_repo_branches(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("repo_branches", err);
            Json(ApiResponse::<RepoBranchesToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_path_search(
    Json(payload): Json<PathSearchToolRequest>,
) -> Json<ApiResponse<PathSearchToolResponse>> {
    match execute_path_search(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("path_search", err);
            Json(ApiResponse::<PathSearchToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_file_content(
    Json(payload): Json<FileContentToolRequest>,
) -> Json<ApiResponse<FileContentToolResponse>> {
    match execute_file_content(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("file_content", err);
            Json(ApiResponse::<FileContentToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_file_list(
    Json(payload): Json<FileListToolRequest>,
) -> Json<ApiResponse<FileListToolResponse>> {
    match execute_file_list(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("file_list", err);
            Json(ApiResponse::<FileListToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_symbol_insights(
    Json(payload): Json<SymbolInsightsToolRequest>,
) -> Json<ApiResponse<SymbolInsightsToolResponse>> {
    match execute_symbol_insights(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("symbol_insights", err);
            Json(ApiResponse::<SymbolInsightsToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

pub async fn tool_repositories(
    Json(payload): Json<RepositoriesToolRequest>,
) -> Json<ApiResponse<RepositoriesToolResponse>> {
    match execute_repositories(payload).await {
        Ok(data) => Json(ApiResponse::success(data)),
        Err(err) => {
            let (code, message, suggestion) = normalize_tool_error("repositories", err);
            Json(ApiResponse::<RepositoriesToolResponse>::failure(
                code, message, suggestion,
            ))
        }
    }
}

enum SearchExecutionMode {
    Single {
        query: String,
        page: u32,
    },
    Batch {
        queries: Vec<String>,
        dedupe: SearchDedupeMode,
        max_results_per_query: usize,
    },
}

fn validate_search_mode(payload: SearchToolRequest) -> Result<SearchExecutionMode, String> {
    let query = payload.query.unwrap_or_default().trim().to_string();
    let queries = payload.queries.unwrap_or_default();
    let has_query = !query.is_empty();
    let has_queries = !queries.is_empty();

    match (has_query, has_queries) {
        (true, true) => Err("provide exactly one of 'query' or 'queries' for search".to_string()),
        (false, false) => {
            Err("missing search input: provide 'query' or non-empty 'queries'".to_string())
        }
        (true, false) => Ok(SearchExecutionMode::Single {
            query,
            page: payload.page.max(1),
        }),
        (false, true) => {
            if queries.len() > MAX_BATCH_QUERIES {
                return Err(format!(
                    "too many queries in batch: max {}",
                    MAX_BATCH_QUERIES
                ));
            }
            let normalized: Vec<String> = queries
                .into_iter()
                .map(|q| q.trim().to_string())
                .filter(|q| !q.is_empty())
                .collect();
            if normalized.is_empty() {
                return Err("queries must contain at least one non-empty query".to_string());
            }
            Ok(SearchExecutionMode::Batch {
                queries: normalized,
                dedupe: payload.dedupe,
                max_results_per_query: payload.max_results_per_query.max(1) as usize,
            })
        }
    }
}

async fn execute_single_search(query: String, page: u32) -> Result<Value, String> {
    let page_data = search(query.clone(), page)
        .await
        .map_err(|err| err.to_string())?;

    let mut freshness = freshness_from_search_results(&page_data.results);
    if freshness.indexed_at.is_none() {
        if let (Some(repo), Some(branch)) = (
            extract_filter_value(&query, "repo"),
            extract_filter_value(&query, "branch"),
        ) {
            freshness = resolve_branch_freshness(&repo, &branch, None)
                .await
                .unwrap_or_else(|_| unknown_freshness());
        }
    }

    let enriched_results = enrich_results(&page_data.results);
    let top_filetypes = compute_top_filetypes(&page_data.results);
    let mut guidance = Vec::new();
    if page_data.results.is_empty() {
        guidance = build_no_result_guidance(&query);
    } else if page_data.has_more {
        guidance.push(
            "Results are truncated for this page. Use truncation.next_page_args to continue."
                .to_string(),
        );
    }

    Ok(json!({
        "mode": "single",
        "query": page_data.query,
        "page": page_data.page,
        "page_size": page_data.page_size,
        "has_more": page_data.has_more,
        "results": enriched_results,
        "stats": page_data.stats,
        "facets": {
            "common_directories": page_data.stats.common_directories,
            "top_repositories": page_data.stats.top_repositories,
            "top_branches": page_data.stats.top_branches,
            "top_filetypes": top_filetypes,
        },
        "index_freshness": freshness,
        "truncation": {
            "has_more": page_data.has_more,
            "next_page_args": if page_data.has_more { json!({"query": query, "page": page + 1}) } else { Value::Null },
        },
        "guidance": guidance,
    }))
}

async fn execute_batch_search(
    queries: Vec<String>,
    dedupe: SearchDedupeMode,
    max_results_per_query: usize,
) -> Result<Value, String> {
    let mut pages: Vec<(String, SearchResultsPage)> = Vec::with_capacity(queries.len());
    for query in &queries {
        let page = search(query.clone(), 1)
            .await
            .map_err(|err| err.to_string())?;
        pages.push((query.clone(), page));
    }

    let mut all_results: Vec<SearchResult> = Vec::new();
    let mut per_query_counts = Vec::new();
    let mut any_has_more = false;

    for (query, mut page) in pages {
        if page.results.len() > max_results_per_query {
            page.results.truncate(max_results_per_query);
        }
        per_query_counts.push(json!({
            "query": query,
            "count": page.results.len(),
            "has_more": page.has_more,
        }));
        any_has_more = any_has_more || page.has_more;
        all_results.extend(page.results);
    }

    let deduped_results = dedupe_results(all_results, dedupe.clone());
    let freshness = freshness_from_search_results(&deduped_results);
    let top_filetypes = compute_top_filetypes(&deduped_results);
    let guidance = if deduped_results.is_empty() {
        vec![
            "No matches found in this batch. Broaden terms or remove restrictive filters."
                .to_string(),
            "For OR semantics, keep separate alternatives in queries and inspect per_query_counts."
                .to_string(),
            "For older snapshots, include historical:yes and rerun per branch.".to_string(),
        ]
    } else {
        Vec::new()
    };

    Ok(json!({
        "mode": "batch",
        "queries": queries,
        "dedupe": dedupe,
        "results": enrich_results(&deduped_results),
        "facets": {
            "top_filetypes": top_filetypes,
        },
        "index_freshness": freshness,
        "batch": {
            "per_query_counts": per_query_counts,
            "deduped_count": deduped_results.len(),
            "truncated": any_has_more,
        },
        "truncation": {
            "has_more": any_has_more,
            "next_step_hint": if any_has_more { "Run single-query search with page>1 for the query of interest." } else { "" }
        },
        "guidance": guidance,
    }))
}

fn dedupe_results(results: Vec<SearchResult>, dedupe: SearchDedupeMode) -> Vec<SearchResult> {
    if dedupe == SearchDedupeMode::None {
        return results;
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for result in results {
        let key = match dedupe {
            SearchDedupeMode::RepoPathLine => format!(
                "{}:{}:{}",
                result.repository, result.file_path, result.match_line
            ),
            SearchDedupeMode::RepoPath => format!("{}:{}", result.repository, result.file_path),
            SearchDedupeMode::None => String::new(),
        };
        if seen.insert(key) {
            out.push(result);
        }
    }
    out
}

fn enrich_results(results: &[SearchResult]) -> Vec<Value> {
    results
        .iter()
        .map(|result| {
            let mut value = serde_json::to_value(result).unwrap_or_else(|_| json!({}));
            if let Some(obj) = value.as_object_mut() {
                let line = result.match_line.max(1);
                let permalink = format!(
                    "/repo/{}/tree/{}/{}#L{}",
                    result.repository, result.commit_sha, result.file_path, line
                );
                obj.insert("permalink".to_string(), json!(permalink));
                obj.insert(
                    "open_at_line".to_string(),
                    json!({
                        "repo": result.repository,
                        "branch_or_commit": result.commit_sha,
                        "path": result.file_path,
                        "line": line,
                    }),
                );
            }
            value
        })
        .collect()
}

fn build_no_result_guidance(query: &str) -> Vec<String> {
    let mut guidance = vec![
        "No results found. Verify repository name with repositories and branch with repo_branches."
            .to_string(),
        "Try broadening the query by removing one filter at a time (path:, file:, lang:, branch:)."
            .to_string(),
    ];
    if query.contains('*') && !query.contains("regex:") {
        guidance.push(
            "Wildcard syntax is not supported in plain terms. Use regex:\"...\" for pattern matching."
                .to_string(),
        );
    }
    if !query.contains("historical:yes") {
        guidance.push(
            "If you are looking for older/newer versions, retry with historical:yes.".to_string(),
        );
    }
    if !query.contains("branch:") {
        guidance.push(
            "For recency checks, run repo_branches and repeat search with explicit branch:<name>."
                .to_string(),
        );
    }
    guidance
}

fn compute_top_filetypes(results: &[SearchResult]) -> Vec<FacetCount> {
    let mut counts: HashMap<String, u32> = HashMap::new();
    for result in results {
        let filetype = result
            .file_path
            .rsplit_once('.')
            .map(|(_, ext)| ext.to_ascii_lowercase())
            .filter(|ext| !ext.is_empty())
            .unwrap_or_else(|| "no_ext".to_string());
        *counts.entry(filetype).or_insert(0) += 1;
    }
    let mut facets: Vec<FacetCount> = counts
        .into_iter()
        .map(|(value, count)| FacetCount { value, count })
        .collect();
    facets.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));
    facets.truncate(10);
    facets
}

async fn resolve_branch_freshness(
    repo: &str,
    branch: &str,
    commit_sha: Option<&str>,
) -> Result<IndexFreshness, String> {
    let branches = get_repo_branches(repo.to_string())
        .await
        .map_err(|err| err.to_string())?;
    let selected = select_branch(&branches, branch, commit_sha);
    Ok(match selected {
        Some(info) => freshness_from_indexed_at(info.indexed_at.clone(), "branch_indexed_at"),
        None => unknown_freshness(),
    })
}

fn select_branch<'a>(
    branches: &'a [RepoBranchDisplay],
    branch: &str,
    commit_sha: Option<&str>,
) -> Option<&'a RepoBranchDisplay> {
    branches
        .iter()
        .find(|b| b.name == branch)
        .or_else(|| commit_sha.and_then(|sha| branches.iter().find(|b| b.commit_sha == sha)))
}

fn freshness_from_search_results(results: &[SearchResult]) -> IndexFreshness {
    let newest = results
        .iter()
        .filter_map(|r| r.snapshot_indexed_at.as_deref())
        .filter_map(parse_utc_datetime)
        .max();
    match newest {
        Some(dt) => freshness_from_indexed_at(Some(dt.to_rfc3339()), "snapshot_indexed_at"),
        None => unknown_freshness(),
    }
}

fn freshness_from_indexed_at(indexed_at: Option<String>, source: &str) -> IndexFreshness {
    match indexed_at {
        Some(ts) => {
            let age_seconds = age_seconds_from_rfc3339(&ts);
            IndexFreshness {
                indexed_at: Some(ts),
                age_seconds,
                age_human: age_seconds.map(format_age_human),
                source: source.to_string(),
            }
        }
        None => unknown_freshness(),
    }
}

fn unknown_freshness() -> IndexFreshness {
    IndexFreshness {
        indexed_at: None,
        age_seconds: None,
        age_human: None,
        source: "unknown".to_string(),
    }
}

fn parse_utc_datetime(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

fn age_seconds_from_rfc3339(raw: &str) -> Option<i64> {
    parse_utc_datetime(raw).map(|dt| (Utc::now() - dt).num_seconds().max(0))
}

fn format_age_human(age_seconds: i64) -> String {
    if age_seconds < 60 {
        format!("{age_seconds}s")
    } else if age_seconds < 3600 {
        format!("{}m", age_seconds / 60)
    } else if age_seconds < 86_400 {
        format!("{}h", age_seconds / 3600)
    } else {
        format!("{}d", age_seconds / 86_400)
    }
}

fn extract_filter_value(query: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}:");
    for token in split_query_tokens(query) {
        if let Some(value) = token.strip_prefix(&prefix) {
            let trimmed = value.trim().trim_matches('"');
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn split_query_tokens(query: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    for c in query.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            ' ' | '\t' if !in_quotes => {
                if !current.trim().is_empty() {
                    tokens.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    if !current.trim().is_empty() {
        tokens.push(current.trim().to_string());
    }

    tokens
}
