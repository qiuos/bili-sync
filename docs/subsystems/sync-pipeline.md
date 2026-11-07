# Sync pipeline

## Responsibility and boundaries

- Owns: the periodic/manual sync run that turns subscribed video sources into database rows and local media files; the per-source stage order (scan → detail fill → danmaku re-sync marks → download); the packed `download_status` bits on `video` and `page`; retry budgeting, per-run concurrency limits, and the risk-control abort decision.
- Does not own: how Bilibili endpoints are signed, authenticated or parsed ([bilibili-client.md](bilibili-client.md)); file layout, naming and NFO/ASS generation ([media-artifacts.md](media-artifacts.md)); where configuration lives and how it is persisted ([config-and-persistence.md](config-and-persistence.md)); HTTP/WebSocket transport ([web-api-server.md](web-api-server.md)); outbound messages ([notifications.md](notifications.md)).
- Talks to: `BiliClient` for remote data, `VersionedConfig` for the current `Config`, `TEMPLATE` cache for path templates, SeaORM `DatabaseConnection`, `Downloader` for byte transfer, `notify`/`error_and_notify` for user-visible results.

## Sources of truth

- Authoritative data: the SQLite tables in `crates/bili_sync_entity/src/entities/` (`video`, `page`, `favorite`, `collection`, `submission`, `watch_later`) plus the packed status columns. The schema is created only by `crates/bili_sync_migration/src/lib.rs`; never create or alter tables from the workflow.
- Authoritative config: `VersionedConfig` (see [config-and-persistence.md](config-and-persistence.md)). The workflow only reads snapshots or guards.
- Authoritative status encoding: `crates/bili_sync/src/utils/status.rs` (`Status`, `VideoStatus`, `PageStatus`, `STATUS_NOT_STARTED`, `STATUS_MAX_RETRY`, `STATUS_OK`, `STATUS_COMPLETED`). Do not re-derive bit offsets elsewhere; `DANMAKU_STATUS_OFFSET` and `VIDEO_PAGE_STATUS_OFFSET` in `crates/bili_sync/src/workflow.rs` are the only named offsets.
- Forbidden duplicate sources: a second in-memory download-state table, a separate "retry counter" column, or re-reading the config file/database directly inside stage helpers.

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/main.rs` (`main`, `init`, `spawn_task`, `handle_shutdown`) starts the HTTP server and the scheduled downloader and ties their lifetimes to a single `CancellationToken`.
  - `crates/bili_sync/src/task/video_downloader.rs` (`video_downloader`, `DownloadTaskManager::{new,start,download_once,download_video_task,check_and_refresh_credential_task}`) owns scheduling, the single-run lock and task status.
  - `crates/bili_sync/src/workflow.rs` (`process_video_source` and its stage functions) owns the pipeline itself.
- Common edit locations:
  - Add or reorder a pipeline stage: `process_video_source` in `workflow.rs`.
  - Add a per-video or per-page artifact: `download_video_pages` / `download_page` in `workflow.rs`, then the artifact layout page.
  - Add a source kind: `crates/bili_sync/src/adapter/mod.rs` (`VideoSource`, `VideoSourceEnum`, `_ActiveModel`) plus `get_enabled_video_sources` in `crates/bili_sync/src/utils/model.rs`.
  - Change selection queries: `crates/bili_sync/src/utils/model.rs` (`filter_unfilled_videos`, `filter_unhandled_video_pages`).
  - Change status semantics: `crates/bili_sync/src/utils/status.rs` and `crates/bili_sync/src/error.rs` (`ExecutionStatus`).
  - Change danmaku re-sync due dates: `crates/bili_sync/src/utils/danmaku_schedule.rs` (`should_sync_danmaku`).

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Inputs: enabled rows in the four source tables, the current `Config`, and remote Bilibili responses.
  - Outputs: inserted/updated `video` and `page` rows, `latest_row_at` per source, files under each source `path`, task status events, optional notifications.
  - Triggers: `config.interval`, which is `Trigger::Interval(secs)` (repeated job, default 1200) or `Trigger::Cron(expr)` (croner expression with a required seconds field, evaluated with `chrono::Local`); the one-shot job added by `DownloadTaskManager::download_once` (API `POST /api/task/download`); the daily credential job `"0 0 1 * * *"`.
- Main execution path (source files and named symbols):
  1. `download_video_task` takes `cx.running.try_lock()`; if the lock is held it logs `上一次视频下载任务尚未结束，跳过本次执行..` and returns without running. It then publishes `TaskStatus { is_running: true, .. }`, snapshots the config, and calls `download_video`.
  2. `download_video` runs `config.check()`, fetches WBI keys and stores the mixin key via `bilibili::set_global_mixin_key`, snapshots `TEMPLATE`, snapshots the `BiliClient` (freezing the rate limiter for this run), loads `get_enabled_video_sources` (shuffled so no source is starved), bails with `没有可用的视频源` when empty, and iterates `process_video_source` per source.
  3. `process_video_source`: `create_dir_all` for the source path (fails early if unwritable) → `refresh_video_source` → `fetch_video_details` → `prepare_danmaku_updates` → unless `--scan-only` (`ARGS.scan_only`), `download_unprocessed_videos` and one `notify` when the summary says it is worth sending.
  4. `refresh_video_source`: reads `latest_row_at`, consumes the source's `VideoInfo` stream through `should_take`/`should_filter`, and inserts in chunks of 30 with `create_videos` (`INSERT … ON CONFLICT DO NOTHING`). Any stream error is recorded and the stream stops; the error is returned *before* `latest_row_at` is updated, so the same window is re-read next run. On success `latest_row_at` is advanced to the newest `release_datetime` seen.
  5. `fetch_video_details`: `filter_unfilled_videos` (`valid = true`, `download_status = 0`, `category = 2`, `single_page IS NULL`) → per video `get_tags` + `get_view_info` with `buffer_unordered(config.concurrent_limit.video)` → batches of 15 are applied in one transaction (`update_video_detail_models`, `set_video_models_invalid`, `create_pages`). A `BiliError::ErrorResponse { code: -404 }` marks the row invalid instead of failing the run. `single_page` is set from `pages.len() == 1`, and `should_download` from the source `Rule` (true when no rule exists).
  6. `prepare_danmaku_updates`: no-op when the policy is disabled, has no milestones, or `skip_option.no_danmaku`. Otherwise it selects single-page, valid, downloadable videos whose pubtime falls inside the policy window (or whose final sync is still pending), keeps only pages whose danmaku subtask is currently `STATUS_OK`, and asks `should_sync_danmaku` per page; for due pages it clears the danmaku bits (`PageStatus::query_builder().reset_subtask(DANMAKU_STATUS_OFFSET)`) and clears the video-level page-download bits (`reset_subtask(VIDEO_PAGE_STATUS_OFFSET)`) in one transaction, returning the affected video ids.
  7. `download_unprocessed_videos`: `filter_unhandled_video_pages` (`valid = true`, completed bit unset, `category = 2`, `single_page IS NOT NULL`, `should_download = true`) → per video `download_video_pages` with video-level concurrency. The source `filter_option` wins over `config.filter_option`. Videos whose `tags` are `NULL` refetch tags before download. Errors that are not risk control are logged and dropped for that round; risk control stops the stream and is re-raised after the loop.
  8. `download_video_pages`: builds/keeps `video.path`, runs the five video-level subtasks concurrently (`fetch_video_poster`, `generate_video_nfo`, `fetch_upper_face`, `generate_upper_nfo`, `dispatch_download_page`), folds their `ExecutionStatus` into `VideoStatus`, and re-raises risk-control failures. Upper artifacts are written only for the first video of each upper during a run (`assigned_upper_ids`).
  9. `download_page`: builds/keeps `page.path`, runs the five page-level subtasks concurrently (`fetch_page_poster`, `fetch_page_video`, `generate_page_nfo`, `fetch_page_danmaku`, `fetch_page_subtitle`), folds results into `PageStatus`, sets `page.path` to the final `*.mp4` path, and stamps `danmaku_last_synced_at = Utc::now()` only when the danmaku result is exactly `ExecutionStatus::Succeeded`.
  10. `dispatch_download_page` runs pages with `config.concurrent_limit.page`, persists updated pages, and returns `ExecutionStatus::Fixed(min_status_of_all_page_subtasks)` so the video-level page subtask always equals the weakest page subtask.
- State ownership, transitions and persistence:
  - `video.download_status` and `page.download_status` are the only persisted progress state, written back by `update_videos_model` / `update_pages_model` after each successful batch.
  - Encoding: five 3-bit subtasks from the least-significant bit. `0b000` = not started, each failure adds one, `0b100` = `STATUS_MAX_RETRY` (4 attempts, no further retries), `0b111` = `STATUS_OK`, bit 31 = `STATUS_COMPLETED` (set when no subtask still needs to run).
  - Video subtask order: 0 cover/poster, 1 detail NFO, 2 upper face, 3 upper NFO, 4 page download. Page subtask order: 0 poster, 1 video, 2 NFO, 3 danmaku, 4 subtitle (matches `crates/bili_sync/src/utils/status.rs` doc comments and the front-end task labels).
  - `ExecutionStatus::Skipped` and `Succeeded` both set `STATUS_OK`; `Failed` increments unless already at `STATUS_MAX_RETRY`; `Ignored` leaves the bits untouched; `Fixed(value)` overwrites the subtask. `Ignored` is produced for permission-denied I/O and for reqwest decode/body/timeout errors, so those are simply retried next round.
  - Per-source `latest_row_at` is the only scan watermark; `page.danmaku_last_synced_at` is the only danmaku watermark.
  - Record deletion through the API (`DELETE /api/videos/{id}`) rolls the owning source's `latest_row_at` back to one second before the deleted video's release time (favorite/watch-later compare `favtime`, collection `pubtime`, submission `ctime`), only ever backwards, so an eligible video can be rediscovered and downloaded on a re-scan. It never moves the watermark forward.

## Constraints and failures

- Invariants:
  - A video is only downloaded when `valid = true`, `category = 2`, `single_page IS NOT NULL` and `should_download = true`; a video is only detailed when `download_status = 0` and `single_page IS NULL`.
  - The video page-download subtask must equal the minimum page subtask; never mark it `STATUS_OK` just because pages returned rows.
  - Only one download run may be active; the lock is never awaited inside the download job.
  - All four source kinds share the same pipeline; differences belong in the adapter (`should_take`, `should_filter`, `filter_expr`, `refresh`), not in `workflow.rs`.
- Failure modes and recovery:
  - Risk control: `BiliError::RiskControlOccurred`, `VideoStreamsEmpty` or `InvalidStatusCode` anywhere in a page/video task aborts the remainder of that source's download stream; `download_video` logs `检测到风控，终止此轮视频下载任务..` and stops the whole round. Nothing is marked failed for that reason; the next scheduled run retries.
  - Ordinary per-video/page failures increment the subtask counter; after 4 failures the subtask is "done" as failed and the row will not be retried until reset through the API (`reset_failed` / `force_reset_failed`).
  - A scan error leaves `latest_row_at` unchanged; a detail error other than `-404` leaves the video unfilled and it is retried next run.
  - Source-level errors are caught per source, notified, and the round continues to the next source.
  - `--scan-only` skips downloads entirely (logs `已开启仅扫描模式，跳过视频下载..`).
- Compatibility / migration behavior:
  - New subtasks added to an existing `Status<5, _>` require a user-triggered `force_reset_failed` (API `reset-status` with `force: true`) to clear stale completed bits; `force_reset_failed` exists exactly for that case.
  - Adding a migration is the only way to change persisted columns; see [config-and-persistence.md](config-and-persistence.md).

## Forbidden patterns and common mistakes

- Forbidden: calling `process_video_source` from API handlers or tests that expect a bounded runtime; the function hits Bilibili and writes files.
- Forbidden: adding `sleep`/retry loops around `process_video_source` to "handle" risk control. The pipeline deliberately stops the round and relies on the scheduler.
- Forbidden: treating `ExecutionStatus::Ignored` as success or failure; it must leave the subtask counter unchanged.
- Common mistake: reading `download_status` as a boolean or as a completed bit only. `STATUS_COMPLETED` says "no subtask needs to run", which includes subtasks that exhausted retries; inspect the five subtask values to know what failed.
- Common mistake: assuming the page-download subtask can be updated independently of the page rows — `dispatch_download_page` computes it from the pages.
- Common mistake: assuming `danmaku_last_synced_at` is refreshed on every run. A skipped danmaku subtask (`Skipped`) does not touch it, so the re-sync policy can page again on a later run.
- Common mistake: assuming collection scans stop early. `collection::Model::should_take` always returns `true`; ordering guarantees only exist for favorite/submission/watch-later, and the submission dynamic API has a special case for a pinned first item (`idx == 0 && use_dynamic_api`).
- Common mistake: expecting `page_size`/`page` to default to 10/0 in the API but reusing those defaults in the pipeline; the pipeline has no paging, it processes whole result sets.

## How to verify

- Unit tests that pin the status contract: `cargo test -p bili_sync status` (serialized SQL fragment `(("page"."download_status" >> 9) & 7) = 7`, reset/force-reset and `Fixed` behavior).
- Danmaku policy tests: `cargo test -p bili_sync danmaku_schedule`.
- Full backend suite and lints used by CI (`.github/workflows/pr-check.yaml`): `cargo test`, `cargo clippy -- -D warnings`, `cargo +nightly fmt --check` (nightly toolchain is required only for `fmt`, see `rustfmt.toml`).
- Manual checks: run with a test config and `-s` (`--scan-only`), then inspect that new `video`/`page` rows appear and `latest_row_at` advanced; re-run and confirm no duplicate rows; use `POST /api/task/download` to observe `TaskStatus` events via `/api/ws` and confirm the second concurrent trigger logs the skip message.
- Manual failure check: point a source at a non-writable path; `create_dir_all` fails and the error is notified while other sources still run.
