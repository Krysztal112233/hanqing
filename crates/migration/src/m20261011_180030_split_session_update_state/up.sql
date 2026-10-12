-- Split the opaque UpdatesState JSONB into scalar columns + a per-channel
-- table, so partial update-state writes become direct UPDATE/UPSERT without
-- read-modify-write under a lock. Existing state is migrated in place.

ALTER TABLE session_update_state RENAME TO session_update_state_legacy;

CREATE TABLE session_update_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    pts INTEGER,
    qts INTEGER,
    date INTEGER,
    seq INTEGER
);

CREATE TABLE session_channel_state (
    channel_id BIGINT PRIMARY KEY,
    pts INTEGER NOT NULL
);

INSERT INTO session_update_state (id, pts, qts, date, seq)
SELECT 1,
       (data->>'pts')::int,
       (data->>'qts')::int,
       (data->>'date')::int,
       (data->>'seq')::int
FROM session_update_state_legacy
WHERE id = 1;

INSERT INTO session_channel_state (channel_id, pts)
SELECT (c->>'id')::bigint, (c->>'pts')::int
FROM session_update_state_legacy,
     jsonb_array_elements(data->'channels') AS c
WHERE id = 1;

DROP TABLE session_update_state_legacy;
