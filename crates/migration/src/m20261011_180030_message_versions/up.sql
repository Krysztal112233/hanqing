-- Edit/delete archival policy: `messages` always holds the latest version
-- (soft markers only); superseded versions live in `message_versions`.

ALTER TABLE messages
    ADD COLUMN edited_at TIMESTAMPTZ,
    ADD COLUMN deleted_at TIMESTAMPTZ;

CREATE TABLE message_versions (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    msg_id BIGINT NOT NULL,
    -- Snapshot of the message as it was before being superseded. `raw`
    -- carries the full TL payload (including that version's own edit_date).
    text TEXT NOT NULL DEFAULT '',
    raw JSONB NOT NULL,
    superseded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT message_versions_message_fkey FOREIGN KEY (chat_id, msg_id)
        REFERENCES messages (chat_id, msg_id) ON DELETE CASCADE
);

CREATE INDEX idx_message_versions_msg ON message_versions (chat_id, msg_id);
