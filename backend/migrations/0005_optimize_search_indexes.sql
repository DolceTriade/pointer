-- Enable pg_trgm extension for similarity matching
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Create a more efficient approach: create a generated column for the converted text
-- This allows us to index the converted text directly instead of calling the function during search
-- First, let's add a column for the converted text if it doesn't exist
ALTER TABLE chunks ADD COLUMN IF NOT EXISTS data_text TEXT 
GENERATED ALWAYS AS (safe_bytea_to_utf8(data, ''::text)) STORED;

-- Create GIN index on the converted text column for efficient trigram matching
CREATE INDEX IF NOT EXISTS idx_chunks_data_text_trgm_gin ON chunks USING gin (data_text gin_trgm_ops);

-- Create GIN index on the content_tsv column for full text search if it doesn't exist
-- This should already exist from the previous migration, but let's ensure it's there
CREATE INDEX IF NOT EXISTS idx_chunks_content_tsv_gin ON chunks USING gin (content_tsv);

-- Also create a combined index that might help with the OR condition
-- CREATE INDEX IF NOT EXISTS idx_chunks_combined ON chunks USING gin (content_tsv, data_text gin_trgm_ops);