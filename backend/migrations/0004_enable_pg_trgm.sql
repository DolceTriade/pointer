-- Enable pg_trgm extension for similarity matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create GIN index on the data column in chunks table for efficient trigram matching using the % operator
-- This will significantly speed up the % (fuzzy match) operator
CREATE INDEX IF NOT EXISTS idx_chunks_data_trgm_gin ON chunks USING gin (safe_bytea_to_utf8(data) gin_trgm_ops);

-- Ensure we have the full-text search index (should already exist from previous migration)
-- CREATE INDEX IF NOT EXISTS idx_chunks_content_tsv_gin ON chunks USING gin (content_tsv);

-- Create a functional index for the similarity comparison if needed
-- This would be for the specific similarity function, but the % operator is more efficient