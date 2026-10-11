default:
    @just --choose

watch:
    watchexec -e rs -r cargo run -p hanqing

# Sign in interactively (one-time per account; session persists in PostgreSQL).
login *args:
    cargo run -p hanqing-cli -- login {{args}}

# Extract media metadata from already-archived messages (idempotent).
scan-media:
    cargo run -p hanqing-cli -- scan-media

gen-entities:
    # Requires the verified SeaORM CLI version:
    #   cargo install sea-orm-cli --version 2.0.1 --locked --force
    sea-orm-cli generate entity -o ./crates/hanqing-database/src/model --entity-format compact --with-serde both

fmt:
    cargo fmt

# Reset the Telegram session (clears session_* tables; archived messages are kept).
reset-session:
    psql "$DATABASE_URL" -c 'TRUNCATE session_dc_home, session_dc_option, session_peer, session_update_state;'

check:
    cargo test --all -r
