default:
    @just --choose

watch:
    watchexec -e rs -r cargo run -p hanqing

gen-entities:
    # Requires the exact SeaORM CLI version matching the workspace sea-orm:
    #   cargo install sea-orm-cli --version 2.0.4 --locked --force
    sea-orm-cli generate entity -o ./crates/hanqing-database/src/model --entity-format compact --with-serde both

fmt:
    cargo fmt

check:
    cargo test --all -r
