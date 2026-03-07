use serde::{Deserialize, Serialize};

use crate::components::code_intel_panel::SymbolInsightsResponse;
use crate::db::{RepoSummary, TreeEntry};
use crate::pages::file_viewer::SymbolInsightsParams;
use crate::pages::repo_detail::RepoBranchDisplay;

pub const API_SURFACE: &str = "mcp/v1";

#[derive(Debug, Serialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T>
where
    T: Serialize,
{
    pub ok: bool,
    pub api_surface: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ApiError>,
}

impl<T> ApiResponse<T>
where
    T: Serialize,
{
    pub fn success(data: T) -> Self {
        Self {
            ok: true,
            api_surface: API_SURFACE,
            data: Some(data),
            error: None,
        }
    }

    pub fn failure(
        code: impl Into<String>,
        message: impl Into<String>,
        suggestion: Option<String>,
    ) -> Self {
        Self {
            ok: false,
            api_surface: API_SURFACE,
            data: None,
            error: Some(ApiError {
                code: code.into(),
                message: message.into(),
                suggestion,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchToolRequest {
    #[serde(default)]
    pub repo: Option<String>,
    #[serde(default)]
    pub branch: Option<String>,
    #[serde(default)]
    pub lang: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub file: Option<String>,
    #[serde(default)]
    pub regex: Option<String>,
    #[serde(default)]
    pub case: Option<SearchCaseMode>,
    #[serde(default)]
    pub historical: Option<bool>,
    #[serde(default)]
    pub all_terms: Vec<String>,
    #[serde(default)]
    pub any_terms: Vec<String>,
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_search_dedupe")]
    pub dedupe: SearchDedupeMode,
    #[serde(default = "default_max_results_per_query")]
    pub max_results_per_query: u32,
}

fn default_page() -> u32 {
    1
}

fn default_search_dedupe() -> SearchDedupeMode {
    SearchDedupeMode::RepoPathLine
}

fn default_max_results_per_query() -> u32 {
    25
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SearchDedupeMode {
    RepoPathLine,
    RepoPath,
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchCaseMode {
    Yes,
    No,
    Auto,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexFreshness {
    pub indexed_at: Option<String>,
    pub age_seconds: Option<i64>,
    pub age_human: Option<String>,
    pub source: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepoBranchesToolRequest {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileContentToolRequest {
    pub repo: String,
    pub branch: String,
    pub path: String,
    #[serde(default)]
    pub start_line: Option<u32>,
    #[serde(default)]
    pub end_line: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathSearchToolRequest {
    pub repo: String,
    pub branch: String,
    pub query: String,
    #[serde(default)]
    pub limit: Option<u16>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub auto_paginate: Option<bool>,
    #[serde(default)]
    pub max_pages: Option<u8>,
    #[serde(default)]
    pub max_total_entries: Option<u16>,
}

#[derive(Debug, Serialize)]
pub struct PathSearchToolResponse {
    pub entries: Vec<TreeEntry>,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pages_fetched: Option<u8>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SymbolInsightsToolRequest {
    pub params: SymbolInsightsParams,
}

#[derive(Debug, Serialize)]
pub struct RepoBranchesToolResponse {
    pub branches: Vec<RepoBranchDisplay>,
    pub index_freshness: Vec<BranchFreshness>,
}

#[derive(Debug, Serialize)]
pub struct FileContentToolResponse {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub language: Option<String>,
    pub content: String,
    pub line_count: usize,
    pub returned_line_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<FileContentSnippet>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Serialize)]
pub struct FileContentSnippet {
    pub start_line: u32,
    pub end_line: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileListToolRequest {
    pub repo: String,
    pub branch: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_file_list_depth")]
    pub depth: u8,
    #[serde(default = "default_file_list_limit")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub auto_paginate: Option<bool>,
    #[serde(default)]
    pub max_pages: Option<u8>,
    #[serde(default)]
    pub max_total_entries: Option<usize>,
}

fn default_file_list_depth() -> u8 {
    1
}

fn default_file_list_limit() -> usize {
    500
}

#[derive(Debug, Serialize)]
pub struct FileListEntry {
    pub name: String,
    pub path: String,
    pub kind: String,
    pub depth: u8,
}

#[derive(Debug, Serialize)]
pub struct FileListToolResponse {
    pub repository: String,
    pub commit_sha: String,
    pub root_path: String,
    pub requested_depth: u8,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_reason: Option<String>,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pages_fetched: Option<u8>,
    pub entries: Vec<FileListEntry>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Serialize)]
pub struct SymbolInsightsToolResponse {
    pub insights: SymbolInsightsResponse,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoriesToolRequest {
    #[serde(default = "default_repositories_limit")]
    pub limit: usize,
}

fn default_repositories_limit() -> usize {
    20
}

#[derive(Debug, Serialize)]
pub struct RepositoriesToolResponse {
    pub repositories: Vec<RepoSummary>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Clone, Serialize)]
pub struct BranchFreshness {
    pub name: String,
    pub indexed_at: Option<String>,
    pub age_seconds: Option<i64>,
    pub age_human: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::SymbolInsightsToolRequest;

    #[test]
    fn symbol_insights_scope_is_case_insensitive() {
        let upper = serde_json::json!({
            "params": {
                "repo": "pointer",
                "branch": "main",
                "symbol": "MyType",
                "scope": "Repository"
            }
        });
        let parsed_upper: SymbolInsightsToolRequest =
            serde_json::from_value(upper).expect("Repository scope should deserialize");
        assert_eq!(parsed_upper.params.scope.as_str(), "repository");

        let lower = serde_json::json!({
            "params": {
                "repo": "pointer",
                "branch": "main",
                "symbol": "MyType",
                "scope": "file"
            }
        });
        let parsed_lower: SymbolInsightsToolRequest =
            serde_json::from_value(lower).expect("file scope should deserialize");
        assert_eq!(parsed_lower.params.scope.as_str(), "file");
    }

    #[test]
    fn symbol_insights_scope_rejects_invalid_values() {
        let invalid = serde_json::json!({
            "params": {
                "repo": "pointer",
                "branch": "main",
                "symbol": "MyType",
                "scope": "repo"
            }
        });
        let err = serde_json::from_value::<SymbolInsightsToolRequest>(invalid)
            .expect_err("invalid scope should fail");
        assert!(err.to_string().contains("invalid scope"));
    }
}
