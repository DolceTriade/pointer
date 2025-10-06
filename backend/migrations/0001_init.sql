CREATE TABLE IF NOT EXISTS content_blobs (
    hash TEXT PRIMARY KEY,
    language TEXT,
    byte_len BIGINT NOT NULL,
    line_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id SERIAL PRIMARY KEY,
    repository TEXT NOT NULL,
    commit_sha TEXT NOT NULL,
    file_path TEXT NOT NULL,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    UNIQUE (repository, commit_sha, file_path)
);

CREATE TABLE IF NOT EXISTS symbols (
    id SERIAL PRIMARY KEY,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    namespace TEXT,
    symbol TEXT NOT NULL,
    fully_qualified TEXT NOT NULL,
    kind TEXT,
    UNIQUE (content_hash, namespace, symbol, kind)
);

CREATE INDEX IF NOT EXISTS idx_symbols_symbol ON symbols (symbol);
CREATE INDEX IF NOT EXISTS idx_symbols_fully_qualified ON symbols (fully_qualified);
CREATE INDEX IF NOT EXISTS idx_symbols_namespace ON symbols (namespace);
CREATE INDEX IF NOT EXISTS idx_symbols_kind ON symbols (kind);
CREATE INDEX IF NOT EXISTS idx_symbols_content_hash ON symbols (content_hash);

CREATE TABLE IF NOT EXISTS references (
    id SERIAL PRIMARY KEY,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    namespace TEXT,
    name TEXT NOT NULL,
    fully_qualified TEXT NOT NULL,
    kind TEXT,
    line INTEGER NOT NULL,
    column INTEGER NOT NULL,
    UNIQUE (content_hash, namespace, name, line, column, kind)
);

CREATE INDEX IF NOT EXISTS idx_references_name ON references (name);
CREATE INDEX IF NOT EXISTS idx_references_namespace ON references (namespace);
CREATE INDEX IF NOT EXISTS idx_references_content_hash ON references (content_hash);

CREATE INDEX IF NOT EXISTS idx_files_content_hash ON files (content_hash);
CREATE INDEX IF NOT EXISTS idx_files_repository_commit ON files (repository, commit_sha);
