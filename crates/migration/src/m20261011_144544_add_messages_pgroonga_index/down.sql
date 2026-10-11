-- Drop the PGroonga index only; the extension itself is intentionally kept
-- (PostgreSQL forbids dropping an extension while other objects depend on it).
DROP INDEX IF EXISTS idx_messages_pgroonga_text;
