-- Deduplicate symbol names to avoid repeated substring scans

CREATE TABLE symbol_names (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    name_lc TEXT NOT NULL,
    UNIQUE (name_lc)
);

CREATE INDEX idx_symbol_names_name_lc_trgm
    ON symbol_names USING gin (name_lc gin_trgm_ops);

CREATE TABLE symbol_name_refs (
    symbol_name_id INTEGER NOT NULL REFERENCES symbol_names(id) ON DELETE CASCADE,
    content_hash TEXT NOT NULL REFERENCES content_blobs(hash) ON DELETE CASCADE,
    UNIQUE (symbol_name_id, content_hash)
);

CREATE INDEX idx_symbol_name_refs_symbol_name_id ON symbol_name_refs (symbol_name_id);
CREATE INDEX idx_symbol_name_refs_content_hash ON symbol_name_refs (content_hash);
