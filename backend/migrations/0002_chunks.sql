CREATE TABLE IF NOT EXISTS chunks (
    hash TEXT PRIMARY KEY,
    algorithm TEXT NOT NULL,
    byte_len INTEGER NOT NULL,
    data BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS file_chunks (
    repository TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    file_path TEXT NOT NULL,
    chunk_order INTEGER NOT NULL,
    chunk_hash TEXT NOT NULL REFERENCES chunks(hash) ON DELETE RESTRICT,
    byte_offset BIGINT NOT NULL,
    byte_len INTEGER NOT NULL,
    start_line INTEGER NOT NULL,
    line_count INTEGER NOT NULL,
    PRIMARY KEY (repository, commit_sha, file_path, chunk_order)
);

CREATE INDEX IF NOT EXISTS idx_file_chunks_chunk_hash ON file_chunks (chunk_hash);
