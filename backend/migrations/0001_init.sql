-- Consolidated schema for Pointer code search engine
-- This replaces all previous migrations with an optimized structure

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements; -- For query performance analysis

-- Table for content blobs (stores original data with metadata)
CREATE TABLE content_blobs (
    hash TEXT PRIMARY KEY,
    language TEXT, -- Programming language or 'BINARY' for binary files
    byte_len BIGINT NOT NULL,
    line_count INTEGER NOT NULL,
    -- New: separate text content for efficient searching
    text_content TEXT, -- NULL for binary files, actual text for text files
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for content_blobs
CREATE INDEX idx_content_blobs_language ON content_blobs (language);
-- GIN index for efficient full-text search on text content
CREATE INDEX idx_content_blobs_text_content_fts ON content_blobs USING gin(to_tsvector('simple', text_content)) WHERE text_content IS NOT NULL;
-- GIN index for efficient trigram search on text content
CREATE INDEX idx_content_blobs_text_content_trgm ON content_blobs USING gin (text_content gin_trgm_ops) WHERE text_content IS NOT NULL;

-- Table for files
CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    repository TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    file_path TEXT NOT NULL,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    UNIQUE (repository, commit_sha, file_path)
);

-- Indexes for files
CREATE INDEX idx_files_content_hash ON files (content_hash);
CREATE INDEX idx_files_repository_commit ON files (repository, commit_sha);
CREATE INDEX idx_files_path ON files (file_path);

-- Table for symbols
CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    namespace TEXT,
    symbol TEXT NOT NULL,
    fully_qualified TEXT NOT NULL,
    kind TEXT,
    UNIQUE (content_hash, namespace, symbol, kind)
);

-- Indexes for symbols
CREATE INDEX idx_symbols_symbol ON symbols (symbol);
CREATE INDEX idx_symbols_fully_qualified ON symbols (fully_qualified);
CREATE INDEX idx_symbols_namespace ON symbols (namespace);
CREATE INDEX idx_symbols_kind ON symbols (kind);
CREATE INDEX idx_symbols_content_hash ON symbols (content_hash);

-- Table for symbol references
CREATE TABLE symbol_references (
    id SERIAL PRIMARY KEY,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    namespace TEXT,
    name TEXT NOT NULL,
    fully_qualified TEXT NOT NULL,
    kind TEXT,
    line_number INTEGER NOT NULL,
    column_number INTEGER NOT NULL,
    UNIQUE (content_hash, namespace, name, line_number, column_number, kind)
);

-- Indexes for symbol references
CREATE INDEX idx_symbol_references_name ON symbol_references (name);
CREATE INDEX idx_symbol_references_namespace ON symbol_references (namespace);
CREATE INDEX idx_symbol_references_content_hash ON symbol_references (content_hash);

-- Table for upload chunks (temporary storage during indexing)
CREATE TABLE upload_chunks (
    upload_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (upload_id, chunk_index)
);

-- Index for upload chunks
CREATE INDEX idx_upload_chunks_upload ON upload_chunks (upload_id);

-- Table for file chunks (for large files split into chunks)
CREATE TABLE file_chunks (
    repository TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    file_path TEXT NOT NULL,
    chunk_order INTEGER NOT NULL,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE RESTRICT,
    byte_offset BIGINT NOT NULL,
    byte_len INTEGER NOT NULL,
    start_line INTEGER NOT NULL,
    line_count INTEGER NOT NULL,
    PRIMARY KEY (repository, commit_sha, file_path, chunk_order)
);

-- Indexes for file_chunks
CREATE INDEX idx_file_chunks_content_hash ON file_chunks (content_hash);
CREATE INDEX idx_file_chunks_file_ident ON file_chunks (repository, commit_sha, file_path);