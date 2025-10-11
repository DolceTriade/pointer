use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "ssr", derive(sqlx::FromRow))]
pub struct FileReference {
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub namespace: Option<String>,
    pub name: String,
    pub kind: Option<String>,
    pub line: i32,
    pub column: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HighlightedLine {
    pub line_number: u32,
    pub segments: Vec<HighlightedSegment>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HighlightedSegment {
    pub text: String,
    pub foreground: Option<String>,
    pub background: Option<String>,
    pub bold: bool,
    pub italic: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenOccurrence {
    pub token: String,
    pub line: u32,
    pub column: u32,
    pub length: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolResult {
    pub symbol: String,
    pub namespace: Option<String>,
    pub kind: Option<String>,
    pub fully_qualified: String,
    pub repository: String,
    pub commit_sha: String,
    pub file_path: String,
    pub language: Option<String>,
    pub references: Option<Vec<ReferenceResult>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceResult {
    pub name: String,
    pub namespace: Option<String>,
    pub kind: Option<String>,
    pub fully_qualified: String,
    pub line: usize,
    pub column: usize,
}
