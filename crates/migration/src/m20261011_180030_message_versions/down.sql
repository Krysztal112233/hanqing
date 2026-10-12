DROP TABLE IF EXISTS message_versions;

ALTER TABLE messages
    DROP COLUMN edited_at,
    DROP COLUMN deleted_at;
