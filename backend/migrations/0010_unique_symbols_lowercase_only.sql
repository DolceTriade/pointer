-- Store only lowercased symbol names in unique_symbols.

ALTER TABLE unique_symbols DROP COLUMN IF EXISTS name;
