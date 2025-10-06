use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContentBlob {
    pub hash: String,
    pub language: Option<String>,
    pub byte_len: i64,
    pub line_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolRecord {
    pub content_hash: String,
    pub namespace: Option<String>,
    pub symbol: String,
    pub fully_qualified: String,
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceRecord {
    pub content_hash: String,
    pub namespace: Option<String>,
    pub name: String,
    pub fully_qualified: String,
    pub kind: Option<String>,
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePointer {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexReport {
    pub content_blobs: Vec<ContentBlob>,
    pub symbol_records: Vec<SymbolRecord>,
    pub file_pointers: Vec<FilePointer>,
    pub reference_records: Vec<ReferenceRecord>,
}
