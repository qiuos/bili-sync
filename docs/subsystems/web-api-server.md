# Web API server

## Responsibility and boundaries

- Owns: the HTTP listener and bind-address fallback, the embedded front-end asset serving, the `/api` router and its authentication middleware, the response envelope and status-code mapping, request validation, the WebSocket endpoint `/api/ws` (subscription protocol, log replay, task status, system info polling) and the exact JSON shapes returned to the browser.
- Does not own: the business behavior behind each endpoint ([sync-pipeline.md](sync-pipeline.md), [config-and-persistence.md](config-and-persistence.md), [bilibili-client.md](bilibili-client.md), [notifications.md](notifications.md)); the SvelteKit app that consumes these endpoints ([web-ui.md](web-ui.md)).
- Talks to: `VersionedConfig` (auth token, bind address), `DatabaseConnection` and `Arc<BiliClient>` injected as axum `Extension`s, `DownloadTaskManager` and `LogHelper` for the WebSocket, and `rust_embed_for_web` for the compiled front-end.

## Sources of truth

- Authoritative routing: `crates/bili_sync/src/api/routes/mod.rs` (`router`, `auth`). Adding an endpoint means adding a module `router()` here.
- Authoritative envelope: `crates/bili_sync/src/api/wrapper.rs` (`ApiResponse`, `ApiError`, `ValidatedJson`) and `crates/bili_sync/src/api/error.rs` (`InnerApiError`).
- Authoritative payloads: `crates/bili_sync/src/api/request.rs` (query/body types) and `crates/bili_sync/src/api/response.rs` (returned types, plus the `serde_*_download_status` serializers).
- Authoritative static serving and bind logic: `crates/bili_sync/src/task/http_server.rs` (`http_server`, `frontend_files`, `Asset`).
- Authoritative WebSocket contract: `crates/bili_sync/src/api/routes/ws/mod.rs` (`EventType`, `ClientEvent`, `ServerEvent`, `WebSocketHandler`) and `ws/log_helper.rs` (`LogHelper`, `MAX_HISTORY_LOGS`).
- Forbidden duplicate sources: a second envelope type, error strings duplicated in handlers instead of `InnerApiError`, or a TypeScript type that contradicts `api/response.rs`.

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/task/http_server.rs` (`http_server`) — starts axum, embeds `web/build`.
  - `crates/bili_sync/src/api/routes/mod.rs` (`router`, `auth`) — path prefix and authentication.
  - Route modules: `config`, `me`, `login`, `video_sources`, `videos`, `dashboard`, `ws`, `task` under `crates/bili_sync/src/api/routes/`.
- Common edit locations:
  - New endpoint: add a handler + `router()` in the matching module and merge it in `routes/mod.rs`.
  - New request type: `api/request.rs`, with `#[derive(Validate)]` and `ValidatedJson` in the handler when validation is needed.
  - New response shape: `api/response.rs`; keep the front-end type in `web/src/lib/types.ts` in sync.
  - Event or subscription change: `api/routes/ws/mod.rs` and `web/src/lib/ws.ts` together.
  - Auth semantics: `auth` in `api/routes/mod.rs` only.
- Endpoint inventory (all prefixed with `/api`):
  - `GET /api/config`, `PUT /api/config`, `POST /api/config/notifiers/ping`.
  - `GET /api/me/favorites`, `GET /api/me/collections`, `GET /api/me/uppers`.
  - `POST /api/login/qrcode/generate`, `GET /api/login/qrcode/poll?qrcode_key=…`.
  - `GET /api/video-sources`, `GET /api/video-sources/details`, `GET /api/video-sources/{type}/default-path`, `PUT|DELETE /api/video-sources/{type}/{id}`, `POST /api/video-sources/{type}/{id}/evaluate`, `POST /api/video-sources/{type}/{id}/full-sync`, `POST /api/video-sources/favorites|collections|submissions`.
  - `GET /api/videos`, `GET|DELETE /api/videos/{id}`, `POST /api/videos/{id}/clear-and-reset-status`, `POST /api/videos/{id}/reset-status`, `POST /api/videos/{id}/update-status`, `POST /api/videos/reset-status`, `POST /api/videos/update-status`.
  - `POST /api/videos/cleanup/preview` returns candidate video IDs (`{ invalid_ids, missing_ids }`) for batch cleanup; the preview adds `skipped_source_paths` for source directories with missing candidates but no confirmed existing regular video file under the source; those missing candidates are kept; `POST /api/videos/cleanup` accepts only IDs confirmed by the user (`{ invalid_ids, missing_ids }`), re-verifies them at execution time, and returns `{ deleted_videos_count, deleted_pages_count, skipped_count }`. Mount-point safety: a stat on a source directory succeeds even after unmounting (leftover empty directory), so selection requires at least one previously downloaded regular file whose canonical path is under the same source; otherwise that source's missing candidates are skipped. This is only a conservative heuristic: an existing file cannot establish that separate nested mounts are available. Both only touch database rows, never local files; missing-candidate deletion rolls back the owning source's `latest_row_at` so a re-scan re-discovers the record (invalid candidates keep the watermark, see [sync-pipeline.md](sync-pipeline.md)). Both (and `DELETE /api/videos/{id}`) reject with a 400 while the download task holds its running lock.
  - `GET /api/videos/{id}/cover` serves an authenticated local image or a JSON 404 when unavailable.
  - `GET|POST /api/organization/rules`, `PUT|DELETE /api/organization/rules/{id}`, `POST /api/organization/rules/{id}/preview`, `POST /api/organization/rules/{id}/execute`. Movie preview can list candidates without IDs; movie execution requires explicit video IDs.
  - `GET /api/organization/directory?path=…` lists subdirectories of an existing absolute server/container path (`{ path, parent, entries: [{ path, name }] }`, directories only, sorted by name); empty `path` starts at `/`. Invalid paths return 400. Behind the same auth middleware; it exposes the server filesystem layout to the logged-in operator, not file contents.
  - `GET /api/dashboard` returns `DashBoardResponse { recent_videos: [RecentVideoInfo] }` — the 5 most-recently-added videos ordered by `created_at` desc then `id` desc; each item carries `id`, `name`, `valid`, `should_download`, the five-slot `download_status` tuple, and the naive-UTC `created_at` string (`YYYY-MM-DD HH:MM:SS`, frontend parses it as UTC). `POST /api/task/download`, `GET /api/ws` (WebSocket upgrade).

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Inputs: HTTP requests with an `Authorization` header (or WebSocket subprotocol), JSON bodies, query parameters.
  - Outputs: `ApiResponse<T>` JSON with the HTTP status equal to `status_code`; embedded static files; WebSocket text frames containing `ServerEvent`.
  - Bind address: `VersionedConfig::get().read().bind_address`; on bind failure it retries `default_bind_address()` (`0.0.0.0:12345`) unless it was already the configured value, logging `绑定到地址 {} 失败：{:#}，尝试绑定到默认地址 {}`; success logs `开始运行管理页：http://{addr}`.
- Main execution path (source files and named symbols):
  1. `http_server` builds `router()` (the whole `/api` tree) and adds `.fallback_service(get(frontend_files).head(frontend_files))`, then `Extension(database_connection)`, `Extension(bili_client)`, `Extension(log_writer)`. Only the `/api` subtree carries the auth middleware; static assets are served without it.
  2. `auth` compares the `Authorization` header to `auth_token` by exact string equality. If that fails it removes `Sec-WebSocket-Protocol`, base64-url-safe-no-pad decodes it, and compares the bytes to the token; on success the protocol header is echoed on the response (required by browsers for a WebSocket subprotocol). Otherwise `ApiResponse::<()>::unauthorized("auth token does not match")`.
  3. Handlers return `Result<ApiResponse<T>, ApiError>`. `ApiError` downcasts `InnerApiError::NotFound` → 404 and `BadRequest` → 400 (both with the error's `Display` text as `message`); every other error becomes 500 with `{:#}` formatting. `ApiResponse::ok` sets `status_code = 200`, `data` present, no `message`; error constructors leave `data` absent and set `message`. Both optional fields are skipped when `None`.
  4. `ValidatedJson<T>` extracts JSON and then runs `validator::Validate`, mapping failures to `InnerApiError::BadRequest(e.to_string())`. `StatusUpdate` restricts `status_index` to `0..=4` and `status_value` to `STATUS_OK` or `STATUS_NOT_STARTED` (`utils::validation::validate_status_value`); path fields use `validate_path` (non-empty absolute).
  5. `frontend_files` resolves the request path; empty or unknown paths fall back to `index.html` (SPA routing). Responses carry `Content-Type` from the embedded file, `Cache-Control: no-cache`, and `ETag` = `content.hash()`. A matching `If-None-Match` returns 304; `HEAD` returns headers only. Debug builds return the raw file; release builds pick `br` then `gzip` from `Accept-Encoding` and return 406 `Client must support gzip or brotli compression` when neither is acceptable.
  6. WebSocket `websocket_handler` upgrades and splits the socket: a sender task forwards `mpsc` events, a receiver task parses `ClientEvent`. `EventType` is `camelCase`: `logs`, `tasks`, `sysInfo`; `ServerEvent` is a `camelCase` enum with the same names (`{"logs": "..."}`, `{"tasks": {...}}`, `{"sysInfo": {...}}`).
  7. On `subscribe logs`, the handler first replays `LogHelper` history (up to `MAX_HISTORY_LOGS = 200` entries) and then chains the broadcast stream (channel capacity 30, created in `main::init`); the replay and live stream are one `ServerEvent::Logs` sequence.
  8. On `subscribe tasks`, it streams `DownloadTaskManager::get().subscribe()` (a `watch` channel) as `TaskStatus { is_running, last_run, last_finish, next_run }`.
  9. On `subscribe sysInfo`, the connection registers in a global `DashMap`. A single process-wide poller is started when the first subscriber arrives and cancelled when the last leaves; it ticks every 2 seconds and does the `sysinfo` refresh on a blocking thread, then fans out to every subscriber. `used_cpu` is the global CPU usage, `process_cpu` is the process usage divided by CPU count, and disk totals sum physical disks excluding `overlay`, `tmpfs`, `sysfs`, `proc`.
- State ownership, transitions and persistence:
  - Nothing in this layer is persisted; it reads the config and database and mutates them only through the owning modules.
  - `SysInfo` timestamps are `Utc::now().timestamp_millis()`.
  - Per-connection state is the registration UUID plus per-subscription `CancellationToken`s, all dropped on disconnect.

## Constraints and failures

- Invariants:
  - Every `/api` handler must be behind `auth`; adding a route outside `router()` bypasses authentication.
  - For JSON envelopes, HTTP status must equal `status_code` in the body; clients rely on both.
  - JSON endpoints under `/api` return `ApiResponse` for front-end error mapping. The authenticated cover endpoint returns image bytes on success and JSON on errors.
  - `download_status` is serialized as a five-element `[u32; 5]` array by `serde_video_download_status`/`serde_page_download_status`; never expose the packed `u32`.
- Failure modes and recovery:
  - Missing/incorrect token → 401 `auth token does not match`. There is no automatic redirect or token prompt on that response; the UI surfaces it as a toast and the user re-enters the token in `/settings`.
  - Bind failure → automatic fallback to `0.0.0.0:12345`; if that also fails the task ends, which cancels the whole program (`main::spawn_task` logs `「HTTP 服务」异常结束…`).
  - Handler errors are logged by the caller chain and returned as 500 with the error display; there is no retry at the HTTP layer.
  - WebSocket drops are handled by the client (`web/src/lib/ws.ts` reconnects up to 5 times); the server simply cancels that connection's tasks.
  - Release builds without `Accept-Encoding` support get 406 instead of uncompressed assets — a reverse proxy must not strip the header.
  - `reset-status` with `force: true` clears stale completed bits; without it, `reset_failed` only resets non-zero, non-OK subtasks.
- Compatibility / migration behavior:
  - Route paths and JSON field names are consumed by the bundled front-end; renaming requires matching `web/src/lib/api.ts`/`types.ts` changes in the same commit.
  - `VideoSourceDetail` and `UpdateVideoSourceRequest` are `camelCase`; most other payloads are snake_case. Keep that split when adding fields.
  - `GET /api/videos` pairs `page`/`page_size`: if either is missing, both defaults are used (`0` and `10`).

## Forbidden patterns and common mistakes

- Forbidden: adding business logic to handlers; handlers should translate the request and delegate to the owning module.
- Forbidden: returning a raw `(StatusCode, Json<…>)` tuple; it breaks the envelope.
- Forbidden: logging or echoing the `auth_token`.
- Common mistake: assuming static assets require the token. Only `/api` is authenticated; the SPA is public and asks for the token itself.
- Common mistake: assuming the token is sent as a Bearer token. It is compared verbatim, so the header must be exactly the token value.
- Common mistake: assuming any `Sec-WebSocket-Protocol` value works; it must be the token encoded with `BASE64_URL_SAFE_NO_PAD`, and the server echoes the header back.
- Common mistake: assuming `/api/videos` filters by local date. `created_from`/`created_to` are compared against `datetime(created_at, 'localtime')`, so the caller supplies local wall-clock strings.
- Common mistake: assuming `GET /api/video-sources` returns persisted watch-later rows. When the table is empty it synthesizes `{ id: 1, name: "稍后再看" }`; deleting `watch_later` is rejected as a bad request.
- Common mistake: assuming `POST /api/video-sources/{type}/{id}/full-sync` only touches the database. With `delete_local: true` it also removes local directories and reports failures in `warnings` instead of failing.
- Common mistake: expecting `SysInfo` to include `used_disk`. The Rust struct has `total_disk`/`available_disk` only (see the front-end page for the consequence).
- Common mistake: expecting the socket to push immediately without a `subscribe` message. Each event type must be subscribed explicitly, and history is replayed only once per subscription.

## How to verify

- Start the binary and check the startup log: `开始运行管理页：http://<addr>`; with a deliberately occupied port, confirm the fallback warning and that the UI still answers on `12345`.
- Auth: `curl -i http://127.0.0.1:12345/api/config` → 401 envelope; repeat with `-H "Authorization: <token>"` → 200 with the config.
- Envelope: request a missing video id and confirm HTTP 404 with `{"status_code":404,"message":"Primary key not found: <id>"}`.
- Validation: `POST /api/videos/1/update-status` with `status_index: 9` or `status_value: 3` → 400 from `ValidatedJson`.
- Static serving: `curl -I` on `/` and on an unknown SPA path (both 200 with an `ETag`), then repeat with `If-None-Match` → 304.
- WebSocket: connect to `ws://127.0.0.1:12345/api/ws?` with `Sec-WebSocket-Protocol: <base64url token>`, send `{"subscribe":"logs"}`, confirm the 200-entry history replay followed by live logs; subscribe `sysInfo` and confirm ~2-second cadence; close the last sysInfo subscriber and confirm polling stops (CPU usage drops).
- Existing automated checks: `cargo test -p bili_sync`, `cargo clippy -- -D warnings`, `cargo +nightly fmt --check`.
