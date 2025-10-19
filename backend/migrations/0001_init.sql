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

CREATE TABLE symbols (
    id SERIAL PRIMARY KEY,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    name TEXT NOT NULL,
    UNIQUE (content_hash, name)
);

-- Indexes for symbols
CREATE INDEX idx_symbols_name ON symbols (name);
CREATE INDEX idx_symbols_content_hash ON symbols (content_hash);

-- Table for symbol namespaces
CREATE TABLE symbol_namespaces (
    id SERIAL PRIMARY KEY,
    namespace TEXT NOT NULL UNIQUE
);

CREATE INDEX idx_symbol_namespaces_namespace ON symbol_namespaces (namespace);

-- Table for symbol references
CREATE TABLE symbol_references (
    id SERIAL PRIMARY KEY,
    symbol_id INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
    namespace_id INTEGER NOT NULL REFERENCES symbol_namespaces(id) ON DELETE CASCADE,
    kind TEXT,
    line_number INTEGER NOT NULL,
    column_number INTEGER NOT NULL,
    UNIQUE (symbol_id, namespace_id, line_number, column_number, kind)
);

-- Indexes for symbol references
CREATE INDEX idx_symbol_references_symbol_id ON symbol_references (symbol_id);
CREATE INDEX idx_symbol_references_namespace_id ON symbol_references (namespace_id);
CREATE INDEX idx_symbol_references_kind ON symbol_references (kind);
CREATE INDEX idx_symbol_references_line_number ON symbol_references (line_number);

-- Function for symbol ranking weight calculations
CREATE OR REPLACE FUNCTION symbol_weight(
    symbol_name TEXT,
    fully_qualified TEXT,
    namespace TEXT,
    symbol_kind TEXT,
    needle TEXT,
    namespace_filter TEXT,
    file_path TEXT,
    path_hint TEXT
) RETURNS DOUBLE PRECISION AS $$
    SELECT
        0::DOUBLE PRECISION
        + CASE
            WHEN symbol_kind = 'definition' THEN 120
            WHEN symbol_kind = 'declaration' THEN 90
            ELSE 50
          END
        + CASE
            WHEN needle IS NULL OR needle = '' THEN 0
            WHEN symbol_name = needle THEN 40
            ELSE 0
          END
        + CASE
            WHEN needle IS NULL OR needle = '' THEN 0
            WHEN fully_qualified = needle THEN 35
            ELSE 0
          END
        + CASE
            WHEN namespace_filter IS NULL OR namespace_filter = '' THEN
                CASE
                    WHEN namespace IS NULL OR namespace = '' THEN 70
                    ELSE -15
                END
            WHEN namespace IS NULL OR namespace = '' THEN -25
            ELSE (
                CASE
                    WHEN namespace = namespace_filter THEN 95
                    WHEN namespace LIKE namespace_filter || '::%' THEN 75
                    WHEN namespace_filter LIKE namespace || '::%' THEN 55
                    ELSE -20
                END
            )
          END
        + CASE
            WHEN path_hint IS NULL OR path_hint = '' THEN 0
            WHEN file_path = path_hint THEN 65
            WHEN file_path LIKE path_hint || '%' THEN 45
            WHEN path_hint LIKE file_path || '%' THEN 35
            ELSE GREATEST(similarity(file_path, path_hint) * 60 - 15, -15)
          END
$$ LANGUAGE SQL IMMUTABLE;

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
