-- Move chunk reference counts into a dedicated table to avoid hot updates on chunks.

CREATE TABLE IF NOT EXISTS chunk_ref_counts (
    chunk_hash TEXT PRIMARY KEY REFERENCES chunks(chunk_hash) ON DELETE CASCADE,
    ref_count INTEGER NOT NULL DEFAULT 0
);

INSERT INTO chunk_ref_counts (chunk_hash, ref_count)
SELECT chunk_hash, COUNT(*) AS ref_count
FROM content_blob_chunks
GROUP BY chunk_hash
ON CONFLICT (chunk_hash) DO UPDATE
SET ref_count = EXCLUDED.ref_count;

DROP TRIGGER IF EXISTS update_chunk_ref_count_trigger ON content_blob_chunks;
DROP FUNCTION IF EXISTS update_chunk_ref_count();

CREATE OR REPLACE FUNCTION update_chunk_ref_count()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO chunk_ref_counts (chunk_hash, ref_count)
        VALUES (NEW.chunk_hash, 1)
        ON CONFLICT (chunk_hash)
        DO UPDATE SET ref_count = chunk_ref_counts.ref_count + 1;
        RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE chunk_ref_counts
        SET ref_count = ref_count - 1
        WHERE chunk_hash = OLD.chunk_hash;
        DELETE FROM chunk_ref_counts
        WHERE chunk_hash = OLD.chunk_hash
          AND ref_count <= 0;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_chunk_ref_count_trigger
AFTER INSERT OR DELETE ON content_blob_chunks
    FOR EACH ROW EXECUTE PROCEDURE update_chunk_ref_count();

ALTER TABLE chunks DROP COLUMN IF EXISTS ref_count;
