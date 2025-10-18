-- Finalized schema for Pointer code search engine
-- This schema supports fully deduplicated, content-defined chunking for all text files.

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Table for content blobs (stores file-level metadata only)
CREATE TABLE content_blobs (
    hash TEXT PRIMARY KEY,
    language TEXT,
    byte_len BIGINT NOT NULL,
    line_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_content_blobs_language ON content_blobs (language);

-- Table for unique, deduplicated content chunks
CREATE TABLE chunks (
    chunk_hash TEXT PRIMARY KEY,
    text_content TEXT NOT NULL
);

-- FTS and Trigram indexes on the deduplicated chunk content
CREATE INDEX idx_chunks_text_content_fts ON chunks USING gin(to_tsvector('simple', text_content));
CREATE INDEX idx_chunks_text_content_trgm ON chunks USING gin (text_content gin_trgm_ops);

-- Join table to map file blobs to their sequence of chunks
CREATE TABLE content_blob_chunks (
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    chunk_hash TEXT NOT NULL REFERENCES chunks(chunk_hash) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    chunk_line_count INTEGER NOT NULL,
    PRIMARY KEY (content_hash, chunk_index)
);

CREATE INDEX idx_content_blob_chunks_chunk_hash ON content_blob_chunks (chunk_hash);

-- Table for files - references a content blob
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
    UNIQUE (content_hash, namespace, symbol)
);

-- Indexes for symbols
CREATE INDEX idx_symbols_symbol ON symbols (symbol);
CREATE INDEX idx_symbols_namespace ON symbols (namespace);
CREATE INDEX idx_symbols_content_hash ON symbols (content_hash);

-- Table for symbol references
CREATE TABLE symbol_references (
    id SERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    kind TEXT,
    line_number INTEGER NOT NULL,
    column_number INTEGER NOT NULL,
    UNIQUE (symbol_id, line_number, column_number, kind)
);

-- Indexes for symbol references
CREATE INDEX idx_symbol_references_symbol_id ON symbol_references (symbol_id);
CREATE INDEX idx_symbol_references_kind ON symbol_references (kind);
CREATE INDEX idx_symbol_references_line_number ON symbol_references (line_number);

-- Table for upload chunks (temporary storage for manifest upload)
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
