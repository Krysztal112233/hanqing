# AGENTS.md

hanqing is a Telegram → PostgreSQL archive daemon: it signs into a personal
Telegram account via MTProto and archives message history into PostgreSQL.

## Design scope and priorities

- Single-instance daemon; prefer simple, clear, easy-to-maintain solutions.
- Multi-instance deployment, distribution, and load balancing are out of scope.

## Crate layout

- `crates/hanqing` — binary (daemon entry); global allocator is `mimalloc`.
- `crates/hanqing-config` — `hanqing.toml` + `HANQING__` env prefix (`__`
  separator); strict parsing (`deny_unknown_fields`), unknown keys are startup
  hard errors.
- `crates/hanqing-database` — SeaORM entities (`src/model/`, generated) and
  connection setup.
- `crates/hanqing-telegram` — `grammers` client wrapper: connect, sign-in,
  message/dialog iteration, media download.
- `crates/migration` — SeaORM migrations, created exclusively via
  `sea-orm-cli migrate generate` (never hand-written files).

## Conventions

- Error handling: `snafu`; per-crate `error.rs`; context selectors are
  `pub(crate)` in libraries and `pub` in the binary (main.rs is a separate
  crate from the lib).
- Messages are stored with the full Telegram payload serialized into a `raw`
  JSONB column; only high-frequency fields (chat_id, msg_id, date, text) are
  promoted to indexed columns, so `grammers` upgrades don't force schema
  changes.
- `grammers-*` crates are pinned by git `rev` from
  <https://codeberg.org/Lonami/grammers> (crates.io releases lag TL layers).
- SeaORM is pinned exactly (`=2.0.4`); `sea-orm-cli` must match (`just
  gen-entities`).
- Telegram API credentials live in `.env` / `HANQING__TELEGRAM__*`, never in
  committed files. Session files (`*.session`) are gitignored.

## Commands

```bash
just watch        # auto-recompile on .rs changes (uses watchexec)
just gen-entities # regenerate entity models from DB via sea-orm-cli
just fmt          # cargo fmt
cargo test        # run all tests (same as `just check`)
```

## Commit conventions

Conventional Commits: `type(scope): description`; scope is the crate name
(`hanqing`, `hanqing-database`, `migration`, ...); subject-only, lowercase
imperative, no trailing period; identifiers/paths in backticks.
