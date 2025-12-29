-- Add normalized symbol names for faster matching and a unique symbol name table.

ALTER TABLE symbols ADD COLUMN name_lc TEXT;

UPDATE symbols SET name_lc = LOWER(name) WHERE name_lc IS NULL;

ALTER TABLE symbols ALTER COLUMN name_lc SET NOT NULL;

CREATE INDEX idx_symbols_name_lc_content_hash ON symbols (name_lc, content_hash);

CREATE TABLE unique_symbols (
    name_lc TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE INDEX idx_unique_symbols_name_lc_trgm
    ON unique_symbols USING gin (name_lc gin_trgm_ops);

INSERT INTO unique_symbols (name_lc, name)
SELECT
    LOWER(name) AS name_lc,
    MIN(name) AS name
FROM symbols
GROUP BY LOWER(name)
ON CONFLICT (name_lc) DO NOTHING;
