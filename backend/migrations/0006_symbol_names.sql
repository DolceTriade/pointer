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

-- Backfill from existing symbols
INSERT INTO symbol_names (name, name_lc)
SELECT
    MIN(name) AS name,
    LOWER(name) AS name_lc
FROM symbols
GROUP BY LOWER(name);

INSERT INTO symbol_name_refs (symbol_name_id, content_hash)
SELECT DISTINCT
    sn.id,
    s.content_hash
FROM symbols s
JOIN symbol_names sn
  ON sn.name_lc = LOWER(s.name);

-- Keep symbol_names and symbol_name_refs in sync with symbols
CREATE OR REPLACE FUNCTION upsert_symbol_name_ref(symbol_name TEXT, hash TEXT)
RETURNS VOID AS $$
DECLARE
    name_id INTEGER;
BEGIN
    INSERT INTO symbol_names (name, name_lc)
    VALUES (symbol_name, LOWER(symbol_name))
    ON CONFLICT (name_lc) DO UPDATE
        SET name = EXCLUDED.name
    RETURNING id INTO name_id;

    INSERT INTO symbol_name_refs (symbol_name_id, content_hash)
    VALUES (name_id, hash)
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_symbol_name_ref(symbol_name TEXT, hash TEXT)
RETURNS VOID AS $$
DECLARE
    name_id INTEGER;
BEGIN
    SELECT id INTO name_id FROM symbol_names WHERE name_lc = LOWER(symbol_name);
    IF name_id IS NULL THEN
        RETURN;
    END IF;

    DELETE FROM symbol_name_refs
    WHERE symbol_name_id = name_id
      AND content_hash = hash;

    DELETE FROM symbol_names
    WHERE id = name_id
      AND NOT EXISTS (
          SELECT 1
          FROM symbol_name_refs
          WHERE symbol_name_id = name_id
      );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION symbols_after_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM upsert_symbol_name_ref(NEW.name, NEW.content_hash);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION symbols_after_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.name = OLD.name AND NEW.content_hash = OLD.content_hash THEN
        RETURN NEW;
    END IF;

    PERFORM cleanup_symbol_name_ref(OLD.name, OLD.content_hash);
    PERFORM upsert_symbol_name_ref(NEW.name, NEW.content_hash);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION symbols_after_delete()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM cleanup_symbol_name_ref(OLD.name, OLD.content_hash);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER symbols_after_insert_trigger
AFTER INSERT ON symbols
FOR EACH ROW EXECUTE FUNCTION symbols_after_insert();

CREATE TRIGGER symbols_after_update_trigger
AFTER UPDATE ON symbols
FOR EACH ROW EXECUTE FUNCTION symbols_after_update();

CREATE TRIGGER symbols_after_delete_trigger
AFTER DELETE ON symbols
FOR EACH ROW EXECUTE FUNCTION symbols_after_delete();
