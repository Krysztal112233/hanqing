-- Rebuild the legacy single-JSONB-row layout.

CREATE TABLE session_update_state_legacy (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    data JSONB NOT NULL
);

INSERT INTO session_update_state_legacy (id, data)
SELECT 1, jsonb_build_object(
    'pts', s.pts,
    'qts', s.qts,
    'date', s.date,
    'seq', s.seq,
    'channels', COALESCE(
        (SELECT jsonb_agg(jsonb_build_object('id', c.channel_id, 'pts', c.pts))
         FROM session_channel_state c),
        '[]'::jsonb))
FROM session_update_state s;

DROP TABLE session_update_state;
DROP TABLE session_channel_state;

ALTER TABLE session_update_state_legacy RENAME TO session_update_state;
