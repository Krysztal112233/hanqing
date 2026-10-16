# AGENTS.md

hanqing is a Telegram -> PostgreSQL archive daemon: it signs into a personal
Telegram account via MTProto and archives message history into PostgreSQL.

## Design scope and priorities

- Single-instance daemon; prefer simple, clear, easy-to-maintain solutions.
- Multi-instance deployment, distribution, and load balancing are out of scope.
- **Single-account** (decided 2026-10-11): one Telegram account per deployment;
  `messages` PK is `(chat_id, msg_id)` without an `account_id` column. Going
  multi-account later requires a PK migration and `[[accounts]]` config — an
  accepted trade-off, not a supported goal now.

## Crate layout

- `crates/hanqing` — binary (daemon entry); global allocator is `mimalloc`.
  Account-agnostic: never prompts, exits with an error when no authorized
  session exists.
- `crates/hanqing-cli` — management CLI (`clap`); see `hanqing-cli -h`.
  Sessions live in PG, so CLI commands can run on any host that reaches the
  database.
- `crates/hanqing-config` — `hanqing.toml` + `HANQING__` env prefix (`__`
  separator); strict parsing (`deny_unknown_fields`), unknown keys are startup
  hard errors.
- `crates/hanqing-database` — SeaORM entities (`src/model/`, generated) and
  connection setup.
- `crates/hanqing-telegram` — `grammers` client wrapper: connect, sign-in,
  message/dialog iteration, session storage, shared retry policy (`retry.rs`).
- `crates/hanqing-archive` — the archive core: backfill and live update
  stream (idempotent upserts, edit/delete versioning).
- `crates/hanqing-worker` — resident background workers, one module per
  worker. Workers self-register into a `linkme` distributed slice
  (`#[distributed_slice(WORKERS)]`); the daemon spawns them all via
  `spawn_all` with a `WorkerContext`. A worker decides itself whether config
  enables it and must never take down archiving (detached tasks, errors
  logged not propagated).
- `crates/migration` — SeaORM migrations, created exclusively via
  `sea-orm-cli migrate generate` (never hand-written files). Library-only;
  applied via `hanqing-cli migrate`. Prefer the schema builder
  (`Table::create()`, `Type::create().as_enum()`, ...) when the DDL is
  expressible; raw `up.sql`/`down.sql` files only for extension-specific
  DDL the builder cannot express (pgmq, pgroonga).

## Conventions

- Error handling: `snafu`; per-crate `error.rs`; context selectors are
  `pub(crate)` in libraries and `pub` in the binary (main.rs is a separate
  crate from the lib).
- DB helpers are `async_trait` traits implemented on the entity (fandwill
  pattern), one trait per table in `hanqing-database/src/helper/`; callers
  `use hanqing_database::helper::prelude::*` then call `Entity::method(db)`
  (e.g. `SyncState::find_pending(db)`).
- Messages are stored with the full Telegram payload serialized into a `raw`
  JSONB column; only high-frequency fields (chat_id, msg_id, date, sender,
  text) are promoted to indexed columns, so `grammers` upgrades don't force
  schema changes.
- The daemon is resident: `run_update_stream` (live updates) runs in the
  main task; backfill is a worker driven by the pgmq `backfill_run` queue —
  each trigger message runs one `backfill_all` pass. The daemon enqueues a
  startup trigger (`HANQING_SKIP_BACKFILL=1` suppresses it, for testing).
  Both archive paths share idempotent upserts.
- Edit/delete policy: `messages` always holds the latest version with soft
  markers (`edited_at`/`deleted_at`); superseded versions are archived into
  `message_versions` (insert-from-current-row, then update, one txn).
- `session_update_state` is split: scalar pts/qts/date/seq row +
  `session_channel_state` (per-channel pts), so partial update-state writes
  are direct UPDATE/UPSERT with no read-modify-write lock.
- `grammers-tl-types` is a direct dependency with the non-default
  `impl-serde` feature so TL types derive `Serialize`/`Deserialize` (feature
  unification makes `grammers_client` re-exports serializable too). Two
  gotchas: `serde_bytes` renders bytes fields as JSON number arrays (fine for
  metadata), and TL enums serialize as externally tagged (`{"Message":
{...}}`) — account for the nesting when querying inside JSONB.
- Media binary payloads never go into JSONB; they belong on the filesystem or
  object storage with a path reference in the database.
- Media pipeline: the `media` table is the registry and final state; the pgmq
  `media_download` queue carries in-flight tasks (visibility timeout = crash
  recovery, `read_ct` = attempt counter). Archiving enqueues row + task in
  one transaction when S3 is configured; the downloader worker consumes it
  and stores objects at `media/<kind>/<file_id>`, re-fetching each message
  before downloading (`file_reference` expires).
- Enum-like DB columns use real PostgreSQL enum types; `sea-orm-cli`
  generates `DeriveActiveEnum` types (`model/sea_orm_active_enums.rs`) and
  helpers use those generated enums directly.
- Config split: files/env hold connectivity and spawn-time worker knobs
  (`telegram.*`, `database.*`, `s3.*`, `worker.*`); behavior settings live
  in the DB — per-account document in `account_settings` (schema =
  `hanqing_config::AccountSettings`, strict serde), per-chat archive switch
  as `sync_state.enabled`.
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
hanqing-cli -h    # management CLI (login / migrate / search / media / ...)
```

## Commit conventions

Conventional Commits: `type(scope): description`; scope is the crate name
(`hanqing`, `hanqing-database`, `migration`, ...); subject-only, lowercase
imperative, no trailing period; identifiers/paths in backticks.
