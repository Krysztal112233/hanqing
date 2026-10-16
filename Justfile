default:
    @just --choose

watch:
    watchexec -e rs -r cargo run -p hanqing

# Sign in interactively (one-time per account; session persists in PostgreSQL).
login *args:
    cargo run -p hanqing-cli -- login {{args}}

# Apply all pending database migrations.
migrate:
    cargo run -p hanqing-cli -- migrate

# Extract media metadata from already-archived messages (idempotent).
scan-media:
    cargo run -p hanqing-cli -- scan-media

# Enqueue all pending media into the download queue (one-time backlog bootstrap).
enqueue-media:
    cargo run -p hanqing-cli -- media enqueue-pending

# Media status counts and download queue metrics.
media-stats:
    cargo run -p hanqing-cli -- media stats

gen-entities:
    # Requires the verified SeaORM CLI version:
    #   cargo install sea-orm-cli --version 2.0.1 --locked --force
    # enum extras: strum::Display with snake_case (variant name == DB value).
    sea-orm-cli generate entity -o ./crates/hanqing-database/src/model --entity-format compact --with-serde both \
      --enum-extra-derives 'strum::Display' --enum-extra-attributes 'strum(serialize_all = "snake_case")'

fmt:
    cargo fmt

# Reset the Telegram session (clears session_* tables; archived messages are kept).
reset-session:
    psql "$DATABASE_URL" -c 'TRUNCATE session_dc_home, session_dc_option, session_peer, session_update_state, session_channel_state;'

check:
    cargo test --all -r
