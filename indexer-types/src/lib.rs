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
    pub name: String,
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
pub struct SymbolNamespaceRecord {
    pub namespace: String,
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
    pub branches: Vec<BranchHead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchSnapshotPolicy {
    pub interval_seconds: u64,
    pub keep_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchPolicy {
    pub latest_keep_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_live: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_policies: Vec<BranchSnapshotPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchHead {
    pub repository: String,
    pub branch: String,
    pub commit_sha: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<BranchPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UniqueChunk {
    pub chunk_hash: String,
    pub text_content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapping {
    pub content_hash: String,
    pub chunk_hash: String,
    pub chunk_index: usize,
    pub chunk_line_count: i32,
}
