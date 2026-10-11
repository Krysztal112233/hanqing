-- Media metadata registry. Extraction happens at archive time (from the
-- message's raw JSONB); downloading is a separate, later concern driven by
-- the `status` queue (`pending` rows only).

CREATE TYPE media_kind AS ENUM ('photo', 'sticker', 'video', 'voice', 'round', 'document');
CREATE TYPE media_status AS ENUM ('pending', 'downloading', 'done', 'failed', 'skipped');

CREATE TABLE IF NOT EXISTS media (
    -- Telegram document/photo id: stable across messages, so re-uploads of
    -- the same file dedupe naturally.
    file_id BIGINT PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    msg_id BIGINT NOT NULL,
    kind media_kind NOT NULL,
    mime TEXT,
    size_bytes BIGINT,
    file_name TEXT,
    status media_status NOT NULL DEFAULT 'pending',
    path TEXT,
    downloaded_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT media_message_fkey FOREIGN KEY (chat_id, msg_id)
        REFERENCES messages (chat_id, msg_id) ON DELETE CASCADE
);

-- Download queue polling: WHERE status = 'pending' AND size_bytes <= cap.
CREATE INDEX IF NOT EXISTS idx_media_pending ON media (status, size_bytes);
