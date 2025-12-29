-- Remove deprecated symbol name tables now replaced by unique_symbols.

DROP TABLE IF EXISTS symbol_name_refs;
DROP TABLE IF EXISTS symbol_names;
