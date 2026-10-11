-- Per-account behavior settings (single JSONB document; the Rust struct in
-- hanqing-config is the schema). Also serves as the account anchor: the
-- daemon refuses to run when the session belongs to a different account.
CREATE TABLE account_settings (
    account_id BIGINT PRIMARY KEY,
    settings JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-chat archive switch. New dialogs register enabled by default; the
-- daemon archives only enabled chats.
ALTER TABLE sync_state ADD COLUMN enabled BOOLEAN NOT NULL DEFAULT true;
