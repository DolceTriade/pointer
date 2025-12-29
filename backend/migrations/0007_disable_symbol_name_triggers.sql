-- Disable symbol_name cache triggers to avoid write-path deadlocks.

DROP TRIGGER IF EXISTS symbols_after_insert_trigger ON symbols;
DROP TRIGGER IF EXISTS symbols_after_update_trigger ON symbols;
DROP TRIGGER IF EXISTS symbols_after_delete_trigger ON symbols;

DROP FUNCTION IF EXISTS symbols_after_insert();
DROP FUNCTION IF EXISTS symbols_after_update();
DROP FUNCTION IF EXISTS symbols_after_delete();
DROP FUNCTION IF EXISTS upsert_symbol_name_ref(TEXT, TEXT);
DROP FUNCTION IF EXISTS cleanup_symbol_name_ref(TEXT, TEXT);
