-- Step 1: Create or Replace the IMMUTABLE function
CREATE OR REPLACE FUNCTION safe_bytea_to_utf8(
    p_bytes bytea,
    p_placeholder text DEFAULT ''
)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN convert_from(p_bytes, 'UTF8');
EXCEPTION
    WHEN OTHERS THEN
        RETURN p_placeholder;
END;
$$;

-- Step 2: Add the generated column using the IMMUTABLE function
ALTER TABLE chunks
ADD COLUMN content_tsv tsvector
GENERATED ALWAYS AS (to_tsvector('simple', safe_bytea_to_utf8(data))) STORED;

-- Step 3: Create the index
CREATE INDEX chunks_content_tsv_idx ON chunks USING GIN(content_tsv);
