# Bilibili client

## Responsibility and boundaries

- Owns: every outbound request to Bilibili — endpoint paths and query parameters, WBI signing, cookie/credential handling and refresh, response-envelope validation, error classification (risk control, invalid status, empty streams), rate limiting and shared HTTP client construction; parsing of source lists, video details, pages, tags, play URLs, subtitles and danmaku segments.
- Does not own: when requests are issued and what is done with the results ([sync-pipeline.md](sync-pipeline.md)); local file layout ([media-artifacts.md](media-artifacts.md)); where the credential is stored ([config-and-persistence.md](config-and-persistence.md)).
- Talks to: `VersionedConfig`/`VersionedCache` for credentials and rate limits, `reqwest` for transport, `sea-orm` only indirectly through `utils::convert` models returned to the pipeline, and `crate::downloader` for byte transfer of already-resolved stream URLs.

## Sources of truth

- Authoritative HTTP behavior: `crates/bili_sync/src/bilibili/client.rs` (client, limiter, snapshot, `request`) and `crates/bili_sync/src/bilibili/mod.rs` (`Validate`, `WbiSign`, `ErrorForStatusExt`, `MIXIN_KEY`).
- Authoritative error taxonomy: `crates/bili_sync/src/bilibili/error.rs` (`BiliError`, `is_risk_control_related`, `is_common_error`). Nothing else may decide what "risk control" means.
- Authoritative endpoint list: the per-area modules under `crates/bili_sync/src/bilibili/` — `video.rs`, `favorite_list.rs`, `collection.rs`, `submission.rs`, `watch_later.rs`, `dynamic.rs`, `me.rs`, `credential.rs`. There is no endpoint registry elsewhere.
- Authoritative stream selection: `crates/bili_sync/src/bilibili/analyzer.rs` (`FilterOption`, `Stream`, `BestStream`, `PageAnalyzer::{streams,best_stream}`, `Stream::urls`).
- Forbidden duplicate sources: re-signing or re-validating responses in the pipeline, hard-coded duplicate endpoint URLs in other modules, or building ad-hoc `reqwest::Client` instances for Bilibili calls.

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/bilibili/client.rs` (`Client::new`, `BiliClient::new`, `BiliClient::snapshot`, `BiliClient::request`, `BiliClient::wbi_img`, `BiliClient::check_refresh`).
  - `crates/bili_sync/src/bilibili/mod.rs` (`set_global_mixin_key`, `Validate::validate`, `WbiSign::wbi_sign`).
  - `crates/bili_sync/src/bilibili/credential.rs` (`Credential::{generate_qrcode,poll_qrcode,need_refresh,refresh,extract}`).
- Common edit locations:
  - New endpoint: add a method to the matching module and reuse `self.client.get(...).wbi_sign()...validate().await`; add any new envelope/field types in the same module.
  - New error condition: `error.rs` variants plus `is_risk_control_related`/`is_common_error`, then callers in `workflow.rs` only if semantics change.
  - Quality/codec/audio preferences: `FilterOption` defaults and `PageAnalyzer::streams`/`best_stream` in `analyzer.rs`.
  - Danmaku appearance: `crates/bili_sync/src/bilibili/danmaku/mod.rs` (`DanmakuOption`), rendering in `canvas.rs`/`ass_writer.rs`.
  - Rate limiting shape: `ConcurrentLimit::rate_limit` in `crates/bili_sync/src/config/item.rs` and `Limiter` in `client.rs`.

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Inputs: a `BiliClient` (limiter mode `Latest(VersionedCache)` for live config or `Snapshot` for a frozen run), the current `Credential`, the global WBI mixin key, and call-specific parameters (fid, sid, mid, bvid, cid, pn, keyword).
  - Outputs: typed JSON models (`VideoInfo`, `PageAnalyzer`, `PageInfo`, tag lists, `SubTitle`, danmaku segments) or a `BiliError`.
  - Interface contract: `Client` sends `Cookie: buvid3=…; SESSDATA=…; bili_jct=…; DedeUserID=…`, a desktop Chrome `User-Agent`, and `Referer: https://www.bilibili.com/`; it enables gzip and uses 10-second connect and read timeouts.
- Main execution path (source files and named symbols):
  1. Every call funnels through `BiliClient::request`, which awaits the `leaky_bucket` limiter built from `ConcurrentLimit::rate_limit` (default `4` permits per `250 ms`, `None` disables it) and then issues the request with the shared `reqwest::Client`.
  2. Requests needing WBI signing call `.wbi_sign()` (`WbiSign`), which sorts all form/query fields, appends `wts` (unix seconds) and `w_rid` (md5 of the query string salted with the mixin key from `MIXIN_KEY: ArcSwapOption<String>`).
  3. `.validate::<T>()` deserializes the JSON envelope `{ code, message, data }`:
     - missing `code` → `BiliError::InvalidResponse`;
     - `code == -352` or a non-null `data.v_voucher` → `BiliError::RiskControlOccurred`;
     - any other non-zero `code` → `BiliError::ErrorResponse { code, message, response }`.
  4. `ErrorForStatusExt` turns HTTP `412` and `403` responses into `BiliError::InvalidStatusCode(status, reason)`, where `reason` is the HTTP status's canonical reason phrase before body validation.
  5. Source enumeration: `favorite_list.rs` (`/x/v3/fav/folder/info`, `/x/v3/fav/resource/list`), `collection.rs` (`/x/series/series`, `/x/series/archives`, `/x/polymer/web-space/seasons_archives_list`), `submission.rs` (`/x/web-interface/card`, `/x/space/wbi/arc/search`), `watch_later.rs` (`/x/v2/history/toview`), `dynamic.rs` (`/x/polymer/web-dynamic/v1/feed/space`). Each module's `VideoInfo` variant maps into `utils::convert` models where validity and time fields are decided.
  6. Details and media: `video.rs` `get_view_info` (`/x/web-interface/wbi/view`), `get_tags` (`/x/web-interface/view/detail/tag`), `get_page_analyzer` (`/x/player/wbi/playurl`, `qn=127`/`fnval=4048`/`fourk=1` → `PageAnalyzer`), `get_subtitles` (`/x/player/wbi/v2`, then downloads each subtitle URL with `https:` prefixed when the URL is protocol-relative), `get_danmaku_writer` (`duration.div_ceil(360)` segments from `/x/v2/dm/wbi/web/seg.so?type=1`, protobuf `DmSegMobileReply`, requires `content-type: application/octet-stream`, segments sorted by progress).
  7. Account data: `me.rs` `get_created_favorites`, `get_followed_collections`, `get_followed_uppers`; each starts with `ensure!(!self.mid().is_empty(), "未获取到用户 ID，请确保填写设置中的 B 站认证信息")`.
- State ownership, transitions and persistence:
  - `MIXIN_KEY` is process-global and replaced once per download round by `set_global_mixin_key`; it is not persisted.
  - `Credential` is the only persisted auth state and is owned by `VersionedConfig`; `refresh` mutates it through `update_credential`, which also bumps the config version so the front-end reloads.
  - Credential refresh sequence: `need_refresh` (expired cookie or `data.refresh` in `/x/web-interface/nav`) → `refresh` requests `https://www.bilibili.com/correspond/1/{correspond_path}` (RSA-OAEP SHA-256 with `ts = now - 20000 ms`) to extract `refresh_csrf` → `/x/passport-login/web/cookie/refresh` with `csrf`/`refresh_csrf`/`refresh_token`/`source=main_web` → `/x/passport-login/web/confirm/refresh`; `extract` requires `SESSDATA`, `bili_jct` and `DedeUserID` from `Set-Cookie` plus a `refresh_token`.
  - QR login is stateless: `generate_qrcode` returns the QR URL and key; `poll_qrcode` maps API codes `0` → success (credential extracted), `86101` → pending, unscanned, `86090` → pending, scanned, `86038` → expired.
  - Per-run snapshots: `BiliClient::snapshot()` freezes the current limiter/config so a config edit mid-run cannot change behavior; calling `snapshot()` on an already-snapshot client is an error (`cannot snapshot a snapshot BiliClient`).

## Constraints and failures

- Invariants:
  - No Bilibili request may bypass `BiliClient::request` (rate limiter) and `.validate::<T>()` (envelope checks).
  - WBI-signed endpoints must sign the exact final query string; adding a query parameter after signing silently breaks the request.
  - `BiliError::RiskControlOccurred`, `VideoStreamsEmpty` and `InvalidStatusCode` are the only risk-control signals (`is_risk_control_related`); do not add new classes without updating that predicate and [sync-pipeline.md](sync-pipeline.md).
  - `BiliError::is_common_error` recognizes only `-503 服务暂不可用` and `-504 服务调用超时`; those are suppressed when `Config::ignore_common_errors` is enabled.
- Failure modes and recovery:
  - Risk control → stream is cut and the round stops (see the pipeline page); the client itself never retries.
  - `VideoStreamsEmpty` when `/dash/video` is absent; usually geo/paid/charging content. It is treated as risk control, not as a per-video failure.
  - `InvalidStatusCode(412|403)` frequently accompanies risk control; the same handling applies.
  - `-404` on `get_view_info` is surfaced as `ErrorResponse { code: -404 }` and the pipeline marks the video invalid rather than failing the round.
  - Credential refresh failures are logged and notified through `check_and_refresh_credential_task`; the program continues with the old credential.
  - Transient network/timeout/decode errors are converted to `ExecutionStatus::Ignored` by `crates/bili_sync/src/error.rs`, so they do not consume retry attempts.
- Compatibility / migration behavior:
  - Bilibili changes endpoints and envelope shapes without notice; new fields should be `Option`/`#[serde(default)]` so a missing field does not fail an entire scan.
  - `Credential` field names are the persisted JSON keys; renaming them breaks existing databases and requires a migration hook in `VersionedConfig`.

## Forbidden patterns and common mistakes

- Forbidden: creating a second `reqwest::Client` for Bilibili traffic; it would bypass the shared cookie jar and timeouts.
- Forbidden: catching `BiliError` and mapping risk control to a generic failure; the pipeline depends on the distinct variant to stop the round.
- Forbidden: putting retry loops inside client methods; retries are modeled by subtask counters in the pipeline.
- Common mistake: assuming `is_upower_exclusive == is_upower_play` is the only validity rule. `utils::convert` combines `state == 0`, that upower equality (unless `try_upower_anyway`), and `redirect_url.is_none()`; each source kind has its own extra rule (for example favorite `attr` `0|4`, watch later `state == 0`).
- Common mistake: assuming `get_pages` is used in production. It is `#[cfg(test)]`-only; the pipeline derives pages from `get_view_info`.
- Common mistake: treating a subtitle URL as absolute. Subtitles may be protocol-relative and are prefixed with `https:` before download; AI subtitles (URL containing `ai_subtitle`) are filtered out.
- Common mistake: expecting one danmaku request per video. Danmaku is requested per 6-minute segment (`duration.div_ceil(360)`) and only when the response declares `application/octet-stream`.
- Common mistake: expecting every notifier-visible failure to be retried; `ExecutionStatus::Ignored` and `ignore_common_errors` both short-circuit.
- Common mistake: assuming stream choice is "best quality wins". `FilterOption` bounds video quality (`video_min_quality`/`video_max_quality`), audio quality (`audio_min_quality`/`audio_max_quality`), the allowed `codecs` list, and has `no_hdr`, `no_dolby_video`, `no_dolby_audio`, `no_hires` switches; `best_stream` then picks among the survivors in `analyzer.rs`.

## How to verify

- Compile-level check that no direct `reqwest` usage escaped: `cargo clippy -p bili_sync -- -D warnings` (unused/duplicate client code tends to surface as warnings here).
- Unit tests: `cargo test -p bili_sync bilibili` runs non-ignored tests matching `bilibili`. `test_video_info_type`, `test_subtitle_parse`, `test_upower_parse`, `test_ep_parse`, `test_best_stream` and `test_qrcode_login_flow` are ignored manual tests; run them individually with `cargo test -p bili_sync <test_name> -- --ignored` only when their network and credential prerequisites are available.
- Manual check with a real credential: `POST /api/login/qrcode/generate` then `GET /api/login/qrcode/poll` through the running server, and confirm `Config.credential` fields change in the database (`config` row id `1`).
- Manual risk-control check: run a full sync while rate-limited; the logs must show `检测到风控，终止此轮视频下载任务..` and no subtask counter increments for the aborted videos.
- Manual stream-selection check: set an unusual `filter_option` (for example `video_max_quality` below 8K, or drop `AV1` from `codecs`) and confirm the downloaded file corresponds to `best_stream` (`Mixed` for flv/html5/episode-try, otherwise `VideoAudio`).
