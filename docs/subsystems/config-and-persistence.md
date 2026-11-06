# Config and persistence

## Responsibility and boundaries

- Owns: the `Config` schema and its validation, the single-row `config` table that stores it, the in-process `VersionedConfig` singleton with optimistic version checking, the `VersionedCache` fan-out used by derived caches (`TEMPLATE`, rate limiter, `BiliClient`), the SQLite connection/migration setup on disk, and the entity/table shapes that everything else reads.
- Does not own: how configuration values are used ([sync-pipeline.md](sync-pipeline.md), [media-artifacts.md](media-artifacts.md), [notifications.md](notifications.md), [web-ui.md](web-ui.md)); HTTP routes that expose config ([web-api-server.md](web-api-server.md)). This page documents the storage contract, not the endpoints.
- Talks to: `main.rs` at startup, the config HTTP route (which calls `VersionedConfig::update`), the credential refresh job (which calls `update_credential`), and every consumer via `VersionedConfig::get().snapshot()`/`read()` or `VersionedCache`.

## Sources of truth

- Authoritative schema: `crates/bili_sync/src/config/current.rs` (`Config`, `Config::check`, `Config::default`, `CONFIG_DIR`). Any new field belongs here plus `web/src/lib/types.ts` for the editor UI.
- Authoritative defaults: `crates/bili_sync/src/config/default.rs` (`default_auth_token`, `default_bind_address`, `default_favorite_path`, `default_collection_path`, `default_submission_path`, `default_time_format`) and the per-type `Default` impls in `crates/bili_sync/src/config/item.rs`.
- Authoritative persistence: `crates/bili_sync/src/utils/model.rs` (`load_db_config`, `save_db_config`) and `crates/bili_sync/src/database.rs` (`database_url`, `database_connection`, `migrate_database`, `setup_database`).
- Authoritative table shapes: the entity modules under `crates/bili_sync_entity/src/entities/` (`video.rs`, `page.rs`, `favorite.rs`, `collection.rs`, `submission.rs`, `watch_later.rs`, `config.rs`), created by `crates/bili_sync_migration/src/lib.rs`. The `config` table itself is one row with `id = 1` and the whole `Config` serialized into `data`.
- Authoritative CLI/env surface: `crates/bili_sync/src/config/args.rs` (`ARGS`, `scan_only`, `log_level`, `disable_credential_refresh`, `config_dir`, `ffmpeg_path`).
- Forbidden duplicate sources: a second config file, environment variables that duplicate config fields, per-module copies of defaults, or writing the `config` row anywhere except `save_db_config`.

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/main.rs` (`init`) → `setup_database(&CONFIG_DIR.join("data.sqlite"))` → `VersionedConfig::init`.
  - `crates/bili_sync/src/config/versioned_config.rs` (`VersionedConfig::{init,get,read,snapshot,subscribe,update,update_credential}`).
  - `crates/bili_sync/src/config/versioned_cache.rs` (`VersionedCache::{new,read,snapshot}`).
  - `crates/bili_sync/src/config/handlebar.rs` (`TEMPLATE: LazyLock<VersionedCache<Handlebars>>`).
- Common edit locations:
  - Add a config field: `Config` + `Default` + `check` in `current.rs`, defaults in `default.rs`/`item.rs`, and the Svelte settings page + `web/src/lib/types.ts`.
  - Add a table/column: new `m2026…` module in `crates/bili_sync_migration/src/lib.rs`, entity update in `crates/bili_sync_entity/src/entities/`, then the consumers.
  - Change validation messages: `Config::check` (the strings are shown verbatim in the UI and tests).
  - Change SQLite pragmas/pool: `database_connection` in `database.rs`.
- Storage locations:
  - Database: `CONFIG_DIR/data.sqlite` (WAL mode, so `-wal`/`-shm` siblings appear). `CONFIG_DIR` is `--config-dir`/`BILI_SYNC_CONFIG_DIR` or `dirs::config_dir()/bili-sync`.
  - Defaults for `upper_path`: `CONFIG_DIR/upper_face` (must be absolute for `check` to pass).
  - No other persistent file is created; QR/refresh state lives only in the `Config` row.

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Startup inputs: CLI args/env, then the `config` row.
  - Mutations: `PUT /api/config` (full `Config` JSON, validated by `Config::check` then `VersionedConfig::update`) and the credential refresh job (`update_credential`).
  - Outputs: `watch::Sender` notifications consumed by `VersionedCache`, which rebuilds `Handlebars` templates, the rate limiter and therefore the `BiliClient` behavior.
- Main execution path (source files and named symbols):
  1. `init` verifies ffmpeg, calls `setup_database`, then `VersionedConfig::init`. `setup_database` creates the parent directory, runs `migrate_database`, then builds the pooled connection.
  2. `migrate_database` deliberately opens a **separate default** `Database::connect` for migrations (a pooled connection caused ordering issues) and first checks for the `m20250613_043257_add_config` row in `seaql_migrations`; if absent it bails with `该版本仅支持从 2.6.x 以上的版本升级…` so pre-2.6 databases are never half-migrated. Otherwise `Migrator::up` applies the 14 migrations in `crates/bili_sync_migration/src/lib.rs`.
  3. `database_connection` sets `max_connections(50)`, `min_connections(5)`, `acquire_timeout(90s)`, `busy_timeout(90s)`, WAL journal, `synchronous=Normal`, statement logging disabled, and `optimize_on_close(true, None)`.
  4. `VersionedConfig::init` loads the row through `load_db_config`. Missing row → `Config::default()`, log `生成 auth_token：…，可使用该 token 登录 web UI，该信息仅在首次运行时打印`, then persist with `save_db_config`. A row that fails to deserialize bails `解析数据库配置失败： {e}`. The loaded `version` is reset to `0`, because it is only a concurrency token.
  5. Each `VersionedConfig` owns `ArcSwap<Config>` plus a `watch` channel. `update` takes `update_lock`, rejects `old_config.version != new_config.version` with `配置版本不匹配，请刷新页面修改后重新提交`, increments the version, saves to the database **before** swapping the in-memory value, then publishes on the watch channel. `update_credential` does the same without the version check input (it reads the current value first).
  6. `VersionedCache::new(builder)` builds the initial value from the current config and spawns a task on the watch channel that rebuilds on every change, logging `Failed to update versioned cache: …` on failure; `Drop` cancels that task.
- State ownership, transitions and persistence:
  - In-memory config is authoritative for running code; the database row is authoritative across restarts. Writes go database-first, so a failed write leaves memory unchanged.
  - `version` is monotonic per successful update and is used only to reject stale edits. It is not persisted meaningfully across restarts (reset to 0).
  - `Config::check` encodings: `upper_path` must be absolute; `video_name`/`page_name` non-empty; all five `Credential` fields non-empty; `concurrent_limit.video > 0 && page > 0`; `Trigger::Interval` must be `> 60` seconds; `Trigger::Cron` must parse with `Seconds::Required` and `dom_and_dow(true)`; `DanmakuUpdatePolicy::validate` requires strictly increasing milestone days and non-zero periodic interval. All errors are joined as `- message` lines.
  - `Trigger` is `#[serde(untagged)]`: a JSON number is `Interval(secs)`, a JSON string is `Cron(expr)`; the UI therefore sends a number or a string depending on the schedule mode.
  - `NFOTimeType` is `#[serde(rename_all = "lowercase")]` → `favtime`/`pubtime`; `Config` is stored as one JSON blob, so renaming a field or enum variant breaks existing databases unless a migration rewrites the row.
  - `Config` serializes `notifiers: Option<Arc<Vec<Notifier>>>` and the rest of the struct with snake_case field names (no `rename_all`).

## Constraints and failures

- Invariants:
  - Exactly one `config` row, `id = 1`; `save_db_config` upserts on that column.
  - Any config read in a long-running operation uses `snapshot()`/`read()`; never call `VersionedConfig::get()` inside a hot loop expecting fresh values per iteration.
  - Schema changes go through a new migration; never `ALTER TABLE` from application code.
  - The `auth_token` is generated once, at first run, and is never logged again. Do not add logging of it.
- Failure modes and recovery:
  - Pre-2.6 database → startup error with the upgrade message (see above); there is no automatic downgrade.
  - Unparsable config JSON → startup fails; restore from a backup or wipe the `config` row to regenerate defaults (which generates a new `auth_token`).
  - Concurrent edits → second `PUT /api/config` fails with the version-mismatch message; the UI must reload.
  - Config validation failure → `update_config` returns the joined `- message` lines as a bad request; memory and database are untouched.
  - `setup_database` failure surfaces as `数据库初始化失败`; permission problems on the config dir surface as `Failed to create config directory…`.
  - A failed `VersionedCache` rebuild only logs; the previous cached value keeps being used.
- Compatibility / migration behavior:
  - Migrations in order: `m20240322_000001_create_table`, `m20240505_130850_add_collection`, `m20240709_130914_watch_later`, `m20240724_161008_submission`, `m20250122_062926_add_latest_row_at`, `m20250612_090826_add_enabled`, `m20250613_043257_add_config`, `m20250712_080013_add_video_created_at_index`, `m20250903_094454_add_rule_and_should_download`, `m20251009_123713_add_use_dynamic_api`, `m20260324_055217_add_staff`, `m20260712_123205_add_filter_option`, `m20260821_025000_mark_empty_tags_for_refetch`, `m20260823_000001_add_danmaku_last_synced_at`.
  - New `Config` fields must have `#[serde(default)]`/defaulted types so existing rows deserialize.
  - `m20260821_025000_mark_empty_tags_for_refetch` exists because previously stored `tags` could be empty; keep that behavior in mind when adding tag-dependent logic.

## Forbidden patterns and common mistakes

- Forbidden: reading `config.json` or any legacy file; configuration has been database-only since 2.6.0 (`docs/frontend.md`).
- Forbidden: mutating the config through `VersionedConfig` internals (for example swapping `inner`) — the watch channel would not fire and derived caches would go stale.
- Forbidden: creating a `VersionedCache` per request; it spawns a task and holds a subscription.
- Common mistake: expecting `Config::default().upper_path` to be relative; it is absolute inside `CONFIG_DIR`, and `check` enforces absolute.
- Common mistake: assuming the version check is a lock. It is optimistic: two clients reading version `N` and both writing will have exactly one succeed.
- Common mistake: assuming `interval` accepts values below the check threshold. The default is `1200` seconds; anything `<= 60` fails validation, but API scans with `scan_only` do not change the schedule.
- Common mistake: expecting the `trigger` semantics of a standard 5-field cron. `CronParser` requires a leading seconds field (`秒 分 时 日 月 周`), evaluated in `chrono::Local`.
- Common mistake: expecting `config`-stored credentials to be encrypted. They are plain JSON — protect the database file.
- Common mistake: adding a field to `Config` but not to the `settings` page; the UI submits the whole object back and would silently reset it to the default on save if the field is missing from the client model.

## How to verify

- Startup and migration checks: `cargo run -- --config-dir /tmp/bili-sync-test` against an empty directory, then inspect `data.sqlite` (`sqlite3 data.sqlite 'select version from seaql_migrations order by version;'` should list all 14 entries) and the single `config` row (`select id, length(data) from config;`).
- Upgrade guard: copy a database without `m20250613_043257_add_config` into a fresh config dir and confirm the exact bail message instead of a partial migration.
- Validation checks: `PUT /api/config` with `upper_path` relative, empty `video_name`, or `interval = 30`; each must return the corresponding Chinese message and leave the stored config unchanged.
- Version check: read the config twice, update once, then replay the first body; the second call must fail with `配置版本不匹配…`.
- Cache propagation: save a new `video_name` template, then trigger a scan and confirm new directories use the new template without a restart.
- Existing automated checks: `cargo test -p bili_sync` (includes `handlebar::tests::test_template_usage` for `PathSafeTemplate`), `cargo clippy -- -D warnings`, `cargo +nightly fmt --check`.
