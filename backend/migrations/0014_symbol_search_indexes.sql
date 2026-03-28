-- Speed up symbol-search reranking queries by indexing content-hash scoped symbol lookups
-- and definition-only symbol reference joins.

CREATE INDEX IF NOT EXISTS idx_symbol_references_definition_symbol_id
    ON symbol_references (symbol_id)
    WHERE kind = 'definition';

CREATE INDEX IF NOT EXISTS idx_symbols_content_hash_name_lc_id
    ON symbols (content_hash, name_lc, id);
