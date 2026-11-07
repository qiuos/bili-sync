# Subsystems

Project-owned navigation for current responsibilities, contracts and source paths. Find the behavior relevant to your task here or search the directory; read only the matching pages and verify the linked source. Do not load every page by default.

| Task / behavior | Responsibility | Reference | Principal sources |
| --- | --- | --- | --- |
| Change sync scheduling, scan/detail/download stages, retries, concurrency or risk-control aborts | Pipeline orchestration, packed `download_status`, per-run locking | [sync-pipeline.md](sync-pipeline.md) | `crates/bili_sync/src/workflow.rs`, `crates/bili_sync/src/task/video_downloader.rs`, `crates/bili_sync/src/utils/status.rs`, `crates/bili_sync/src/utils/model.rs` |
| Add or fix a Bilibili endpoint, WBI signing, credential/QR login, stream selection, danmaku fetch | Remote client, auth, error taxonomy | [bilibili-client.md](bilibili-client.md) | `crates/bili_sync/src/bilibili/`, `crates/bili_sync/src/bilibili/error.rs`, `crates/bili_sync/src/bilibili/analyzer.rs` |
| Change download or hard-link organization layout, NFO content, danmaku rendering, subtitle output or byte transfer | Media artifacts and downloader | [media-artifacts.md](media-artifacts.md) | `crates/bili_sync/src/workflow.rs`, `crates/bili_sync/src/organization.rs`, `crates/bili_sync/src/utils/nfo.rs`, `crates/bili_sync/src/downloader.rs`, `crates/bili_sync/src/config/item.rs` |
| Add a config field, change validation/defaults, migrations, SQLite pragmas or entity columns | Configuration and persistence | [config-and-persistence.md](config-and-persistence.md) | `crates/bili_sync/src/config/`, `crates/bili_sync/src/database.rs`, `crates/bili_sync_migration/src/lib.rs`, `crates/bili_sync_entity/src/entities/` |
| Add or change an HTTP endpoint, auth, response envelope, validation, WebSocket events or embedded front-end serving | Admin API server | [web-api-server.md](web-api-server.md) | `crates/bili_sync/src/api/`, `crates/bili_sync/src/task/http_server.rs` |
| Change Telegram/webhook/SMTP delivery, message wording, digests or suppression rules | Outbound notifications | [notifications.md](notifications.md) | `crates/bili_sync/src/notifier/`, `crates/bili_sync/src/utils/notify.rs`, `crates/bili_sync/src/config/handlebar.rs` |
| Change a screen, navigation, API client, socket client, status display or settings form | SvelteKit admin front-end | [web-ui.md](web-ui.md) | `web/src/routes/`, `web/src/lib/api.ts`, `web/src/lib/ws.ts`, `web/src/lib/stores/filter.ts` |

`pkqcode update` never overwrites this README or the individual pages.
