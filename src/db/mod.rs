pub mod models;
#[cfg(feature = "ssr")]
pub mod postgres;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::db::models::{FileReference, HighlightedLine, ReferenceResult, SearchResult, SymbolResult, TokenOccurrence};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnippetRequest {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub line: u32,
    pub context: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnippetResponse {
    pub start_line: u32,
    pub highlight_line: u32,
    pub total_lines: u32,
    pub lines: Vec<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolReferenceRequest {
    pub repository: String,
    pub commit_sha: String,
    pub fully_qualified: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolReferenceResponse {
    pub references: Vec<FileReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequest {
    pub q: Option<String>,
    pub name: Option<String>,
    pub name_regex: Option<String>,
    pub namespace: Option<String>,
    pub namespace_prefix: Option<String>,
    pub kind: Option<Vec<String>>,
    pub language: Option<Vec<String>>,
    pub repository: Option<String>,
    pub commit_sha: Option<String>,
    pub path: Option<String>,
    pub path_regex: Option<String>,
    pub include_references: Option<bool>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    pub symbols: Vec<SymbolResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoTreeQuery {
    pub commit: String,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeResponse {
    pub repository: String,
    pub commit_sha: String,
    pub path: String,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TreeEntry {
    pub name: String,
    pub path: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileContentResponse {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub language: Option<String>,
    pub lines: Vec<HighlightedLine>,
    pub tokens: Vec<TokenOccurrence>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawFileContent {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub content: String,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepoSummary {
    pub repository: String,
    pub file_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbUniqueChunk {
    pub chunk_hash: String,
    pub text_content: String,
}

#[async_trait]
pub trait Database: Clone + Send + Sync + 'static {
    // Repository and Branch operations
    async fn get_all_repositories(&self) -> Result<Vec<RepoSummary>, DbError>;
    async fn get_branches_for_repository(&self, repository: &str) -> Result<Vec<String>, DbError>;

    // Existing backend operations
    async fn chunk_need(&self, hashes: Vec<String>) -> Result<Vec<String>, DbError>;
    async fn chunk_upload(&self, chunks: Vec<DbUniqueChunk>) -> Result<(), DbError>;
    async fn store_manifest_chunk(
        &self,
        upload_id: String,
        chunk_index: i32,
        total_chunks: i32,
        data: Vec<u8>,
    ) -> Result<(), DbError>;
    async fn finalize_manifest(
        &self,
        upload_id: String,
        compressed: Option<bool>,
    ) -> Result<(), DbError>;
    async fn list_commits(&self, repository: &str) -> Result<Vec<String>, DbError>;
    async fn get_repo_tree(
        &self,
        repository: &str,
        query: RepoTreeQuery,
    ) -> Result<TreeResponse, DbError>;
    async fn get_file_content(
        &self,
        repository: &str,
        commit_sha: &str,
        file_path: &str,
    ) -> Result<RawFileContent, DbError>;
    async fn get_file_snippet(&self, request: SnippetRequest) -> Result<SnippetResponse, DbError>;
    async fn get_symbol_references(
        &self,
        request: SymbolReferenceRequest,
    ) -> Result<SymbolReferenceResponse, DbError>;
    async fn search_symbols(&self, request: SearchRequest) -> Result<SearchResponse, DbError>;
    async fn text_search(&self, query: &str) -> Result<Vec<SearchResult>, DbError>;
    async fn health_check(&self) -> Result<String, DbError>;
}


#[derive(Debug)]
pub enum DbError {
    Database(String),
    Serialization(String),
    Compression(String),
    Internal(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbError::Database(msg) => write!(f, "Database error: {}", msg),
            DbError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            DbError::Compression(msg) => write!(f, "Compression error: {}", msg),
            DbError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DbError {}