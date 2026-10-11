-- PGroonga full-text index on message text for fast CJK search.
-- PGroonga's default bigram tokenizer handles CJK without extra config.
CREATE EXTENSION IF NOT EXISTS pgroonga;

CREATE INDEX IF NOT EXISTS idx_messages_pgroonga_text
    ON messages
    USING pgroonga (text);
