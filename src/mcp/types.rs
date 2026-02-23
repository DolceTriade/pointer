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

#[derive(Debug, Deserialize)]
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
pub struct RepoBranchesToolRequest {
    pub repo: String,
}

#[derive(Debug, Deserialize)]
pub struct FileContentToolRequest {
    pub repo: String,
    pub branch: String,
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct PathSearchToolRequest {
    pub repo: String,
    pub branch: String,
    pub query: String,
    #[serde(default)]
    pub limit: Option<u16>,
}

#[derive(Debug, Serialize)]
pub struct PathSearchToolResponse {
    pub entries: Vec<TreeEntry>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Deserialize)]
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
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Deserialize)]
pub struct FileListToolRequest {
    pub repo: String,
    pub branch: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_file_list_depth")]
    pub depth: u8,
    #[serde(default = "default_file_list_limit")]
    pub limit: usize,
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
    pub entries: Vec<FileListEntry>,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Serialize)]
pub struct SymbolInsightsToolResponse {
    pub insights: SymbolInsightsResponse,
    pub index_freshness: IndexFreshness,
}

#[derive(Debug, Deserialize)]
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
