-- pgmq-backed dispatch queue for media downloads. The `media` table stays
-- the registry and final state; pgmq carries in-flight tasks (visibility
-- timeout for crash recovery, read_ct for attempt counting).
CREATE EXTENSION IF NOT EXISTS pgmq;

SELECT pgmq.create ('media_download');
