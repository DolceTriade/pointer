-- Add a reference count to the chunks table
ALTER TABLE chunks ADD COLUMN ref_count INTEGER NOT NULL DEFAULT 0;

-- Initialize the reference count for existing chunks
UPDATE chunks
SET ref_count = (
    SELECT count(*)
    FROM content_blob_chunks
    WHERE content_blob_chunks.chunk_hash = chunks.chunk_hash
);

-- Add a trigger to automatically update the reference count
CREATE OR REPLACE FUNCTION update_chunk_ref_count()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        UPDATE chunks SET ref_count = ref_count + 1 WHERE chunk_hash = NEW.chunk_hash;
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE chunks SET ref_count = ref_count - 1 WHERE chunk_hash = OLD.chunk_hash;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_chunk_ref_count_trigger
AFTER INSERT OR DELETE ON content_blob_chunks
    FOR EACH ROW EXECUTE PROCEDURE update_chunk_ref_count();