use serde::{Deserialize, Serialize};

// Represents a file's metadata. Content is stored separately.
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
    pub name: String,
    pub fully_qualified: String,
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

// A report containing all the metadata extracted from a repository.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IndexReport {
    pub content_blobs: Vec<ContentBlob>,
    pub symbol_records: Vec<SymbolRecord>,
    pub file_pointers: Vec<FilePointer>,
    pub reference_records: Vec<ReferenceRecord>,
    pub branches: Vec<BranchHead>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchHead {
    pub repository: String,
    pub branch: String,
    pub commit_sha: String,
}

// A unique, deduplicated chunk of text content.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UniqueChunk {
    pub chunk_hash: String,
    pub text_content: String,
}

// Maps a file's content hash to a sequence of chunks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMapping {
    pub content_hash: String,
    pub chunk_hash: String,
    pub chunk_index: usize,
    pub chunk_line_count: i32,
}

// The final output of the indexer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IndexArtifacts {
    pub report: IndexReport,
    pub unique_chunks: Vec<UniqueChunk>,
    pub chunk_mappings: Vec<ChunkMapping>,
}
