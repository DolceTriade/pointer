-- Parallel rebuild for symbol_names + symbol_name_refs with shard workers.
--
-- Usage example (N=8 shards):
--   psql ... -v shard_count=8 -v shard=0 -f backend/sql/rebuild_symbol_name_cache.sql
--   psql ... -v shard_count=8 -v shard=1 -f backend/sql/rebuild_symbol_name_cache.sql
--   ...
--   psql ... -v shard_count=8 -v shard=7 -f backend/sql/rebuild_symbol_name_cache.sql
--
-- After all shard runs, execute the "Finalize" section once.

\echo '=== Rebuild symbol_names + symbol_name_refs (shard ' :shard ' of ' :shard_count ') ==='

-- Create staging tables once (safe to run multiple times).
CREATE TABLE IF NOT EXISTS symbol_names_new (LIKE symbol_names INCLUDING ALL);
CREATE TABLE IF NOT EXISTS symbol_name_refs_new (LIKE symbol_name_refs INCLUDING ALL);

-- Ensure staging tables are empty for a fresh rebuild.
-- Uncomment if you want a clean rebuild on each shard run.
-- TRUNCATE symbol_names_new;
-- TRUNCATE symbol_name_refs_new;

-- Build symbol_names for this shard.
INSERT INTO symbol_names_new (name, name_lc)
SELECT
    MIN(name) AS name,
    name_lc
FROM (
    SELECT
        name,
        LOWER(name) AS name_lc
    FROM symbols
    WHERE MOD(ABS(hashtext(LOWER(name))), :shard_count) = :shard
) t
GROUP BY name_lc;

-- Build symbol_name_refs for this shard.
INSERT INTO symbol_name_refs_new (symbol_name_id, content_hash)
SELECT
    sn.id,
    s.content_hash
FROM symbols s
JOIN symbol_names_new sn
  ON sn.name_lc = LOWER(s.name)
WHERE MOD(ABS(hashtext(LOWER(s.name))), :shard_count) = :shard
GROUP BY sn.id, s.content_hash;

-- Finalize (run once after all shard runs complete).
--
-- CREATE INDEX symbol_names_new_name_lc_trgm
--     ON symbol_names_new USING gin (name_lc gin_trgm_ops);
-- CREATE INDEX symbol_name_refs_new_symbol_name_id
--     ON symbol_name_refs_new (symbol_name_id);
-- CREATE INDEX symbol_name_refs_new_content_hash
--     ON symbol_name_refs_new (content_hash);
--
-- ANALYZE symbol_names_new;
-- ANALYZE symbol_name_refs_new;
--
-- BEGIN;
-- ALTER TABLE symbol_name_refs RENAME TO symbol_name_refs_old;
-- ALTER TABLE symbol_names RENAME TO symbol_names_old;
-- ALTER TABLE symbol_names_new RENAME TO symbol_names;
-- ALTER TABLE symbol_name_refs_new RENAME TO symbol_name_refs;
-- COMMIT;
--
-- DROP TABLE symbol_name_refs_old;
-- DROP TABLE symbol_names_old;
