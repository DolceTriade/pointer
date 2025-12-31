-- Drop unused trigram index on symbols.name

DROP INDEX IF EXISTS idx_symbols_name_trgm;
