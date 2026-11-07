# Notifications

## Responsibility and boundaries

- Owns: the `Notifier` configuration variants (Telegram, Webhook, SMTP), message rendering (Telegram text/photo, webhook handlebars payloads and headers, SMTP MIME structure), the `Message` payload, the "new videos downloaded" digest (`DownloadNotifyInfo`), the decision to send or suppress, and the test-send endpoint behavior.
- Does not own: the config storage format ([config-and-persistence.md](config-and-persistence.md)); when errors occur ([sync-pipeline.md](sync-pipeline.md), [bilibili-client.md](bilibili-client.md)); the settings UI ([web-ui.md](web-ui.md)); the HTTP route plumbing beyond its payload ([web-api-server.md](web-api-server.md)).
- Talks to: `Config::notifiers`, `TEMPLATE` (webhook templates), the shared `reqwest::Client` from `BiliClient::inner_client()`, and the call sites `notify` / `error_and_notify` in `utils/notify.rs`.

## Sources of truth

- Authoritative types and transports: `crates/bili_sync/src/notifier/mod.rs` (`Notifier`, `SmtpEncryption`, `webhook_template_key`, `webhook_template_content`, `Notifier::notify`, `notifier::NotifierAllExt::notify_all`).
- Authoritative payload/format: `crates/bili_sync/src/notifier/message.rs` (`Message`, conversions) and `crates/bili_sync/src/notifier/info.rs` (`DownloadNotifyInfo::{new,record,should_notify}`).
- Authoritative send policy: `crates/bili_sync/src/utils/notify.rs` (`notify`, `error_and_notify`).
- Authoritative webhook template registration: `crates/bili_sync/src/config/handlebar.rs` (`create_template` registers every webhook under `payload_{url}`).
- Forbidden duplicate sources: hard-coded webhook bodies in call sites, a second HTTP client for notifications, or per-run copies of the notifier list (it is snapshotted from the config).

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/notifier/mod.rs` (`Notifier::notify`, `notify_all`).
  - `crates/bili_sync/src/utils/notify.rs` (`notify`, `error_and_notify`).
  - `crates/bili_sync/src/api/routes/config/mod.rs` (`ping_notifiers`, route `POST /api/config/notifiers/ping`).
- Common edit locations:
  - New channel: add a `Notifier` variant, a `notify_internal` arm, the settings form in `web/src/routes/settings/NotifierDialog.svelte`, and the `Notifier` type in `web/src/lib/types.ts`.
  - New digest wording: `Message` conversions in `message.rs` and the switch threshold in `DownloadNotifyInfo::record`.
  - Webhook default body/template: `webhook_template_content` and `create_template`.
  - Suppression rules: `error_and_notify` and `BiliError::is_common_error`.
- Call sites:
  - `workflow::process_video_source` sends one digest per source after downloading (`notify` when `should_notify()`).
  - `task::video_downloader` sends failures for: scheduler init (`初始化视频下载任务失败…`), job reload (`重载视频下载任务失败…`), credential check (`本轮凭据检查与刷新任务执行遇到错误…`), a failed round (`本轮视频下载任务执行遇到错误…`), and a per-source failure (`处理 {} 时遇到错误：{:#}，跳过该视频源`).

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Inputs: `Config::notifiers: Option<Arc<Vec<Notifier>>>` and a `Message { message, image_url }`.
  - Outputs: side effects only — HTTP requests to Telegram/webhook endpoints or an SMTP conversation. `notify` returns `()`; `notify_all` returns `Result<()>` that is always `Ok`.
  - Templates: webhook bodies are handlebars templates registered under `payload_{url}` from `Config.notifiers` whenever the config changes.
- Main execution path (source files and named symbols):
  1. `notify(config, bili_client, msg)` returns immediately when `notifiers` is `None` or empty; otherwise it clones the notifier list and the `reqwest::Client` and spawns `notify_all` as a detached task (fire-and-forget; the sync pipeline never waits for delivery).
  2. `error_and_notify(config, bili_client, msg, error)` logs the message at `error!` first, then returns early when `config.ignore_common_errors` is true **and** any cause in the error chain is a `BiliError` recognized by `is_common_error` (`-503 服务暂不可用`, `-504 服务调用超时`). Otherwise it spawns `notify_all` the same way.
  3. `notify_all` runs all notifiers with `future::join_all` and discards each result, so one failing channel never blocks or reports against another. Delivery failures are not surfaced anywhere except inside each transport's `?` (which becomes a dropped `Result`).
  4. Telegram: with an `image_url` and `skip_image == false`, a `sendPhoto` form (`chat_id`, `photo`, `caption`); otherwise `sendMessage` (`chat_id`, `text`).
  5. Webhook: renders the payload — cached template `payload_{url}` normally, or an inline render of `webhook_template_content(template)` when `ignore_cache` is set (the test endpoint sets it). The default template is <code v-pre>{"text": "{{{message}}}"}</code>. Always sets `Content-Type: application/json`, then applies any custom `headers` that parse as valid `HeaderName`/`HeaderValue` (invalid pairs are silently skipped).
  6. SMTP: builds a lettre message with subject `BiliSync 通知`. With an `image_url`, it downloads the image (requires a `Content-Type`), embeds it as an inline attachment with a random `cid`, and sends `multipart/alternative` (plain text + `multipart/related` HTML with the escaped message and an `<img src="cid:…">`); without an image it sends a plain-text-only message. Transport choice: `None` → `builder_dangerous(host)`, `Tls` → `relay(host)`, `StartTls` → `starttls_relay(host)`, always `.port(port)`. Credentials are added only when `username` or `password` is non-empty.
  7. The test endpoint `POST /api/config/notifiers/ping` deserializes a single `Notifier`, forces `ignore_cache` for webhooks, and sends `This is a test notification from BiliSync.` with the project's socialify image URL, returning the transport error as an `ApiError` (unlike production sends).
  8. `DownloadNotifyInfo` accumulates the digest per source: `record` keeps only models whose five subtask values are all `STATUS_OK` and that are not in `excluded_video_ids` (videos that were only re-synced for danmaku). Beyond 10 titles it collapses to a `Summary` count. `should_notify` is false only while still in list mode with no titles.
- State ownership, transitions and persistence:
  - Notifier configuration lives only in `Config`; nothing about delivery is persisted (no retry queue, no last-sent marker).
  - `DownloadNotifyInfo::List` → `Summary` is the only internal state transition, triggered by the 10-title threshold; subsequent records only increment the count.
  - `Message` is built from either a `&str`/`String` (no image) or a `DownloadNotifyInfo` (optional cover image).

## Constraints and failures

- Invariants:
  - Notifications must never block or fail the sync pipeline: both `notify` and `error_and_notify` spawn detached tasks.
  - The digest counts only videos whose five subtasks are all `STATUS_OK`; a video with a failed artifact must not be announced.
  - Videos that were re-synced only for danmaku (`excluded_video_ids`) must not be announced as new.
  - Webhook payloads are handlebars templates; escaping depends on the template author (<code v-pre>{{{message}}}</code> is unescaped).
- Failure modes and recovery:
  - Delivery errors are swallowed by `notify_all`; users see nothing but the server log line from `error_and_notify` (if any). There is no retry — the next run produces a new digest for new videos only.
  - A malformed webhook template makes `create_template` fail. If this occurs during `TEMPLATE` initialization, initialization fails; on a later config update, `VersionedCache` logs the error and retains the previous template cache. A notification using that stale cache can fail to render, and `notify_all` discards the delivery error.
  - SMTP with a missing image `Content-Type` fails that send (`Missing image Content-Type`); a `None`-encryption server without authentication is supported via `builder_dangerous`.
  - `ignore_common_errors` suppresses only `-503`/`-504`; all other errors are notified.
- Compatibility / migration behavior:
  - `Notifier` is `#[serde(tag = "type", rename_all = "camelCase")]`, stored inside the config JSON blob. Variant/field renames break existing databases; add `#[serde(default)]` for new fields.
  - `SmtpEncryption` serializes as `none`/`tls`/`starttls` (`rename_all = "lowercase"`).
  - The webhook `ignore_cache` field is `#[serde(skip)]` and exists only for the test path; it must not be persisted or sent by clients.

## Forbidden patterns and common mistakes

- Forbidden: awaiting `notify_all` inline in the pipeline or returning its error to the caller; delivery is best-effort by design.
- Forbidden: sending the `auth_token`, cookies or the full config through notifiers. Only the explicit `Message` is sent.
- Forbidden: adding a per-notifier retry loop; there is no durable queue, and a retry would duplicate digests.
- Common mistake: expecting `notify_all` to report failures. It always returns `Ok(())`; only `Notifier::notify` (used by the ping endpoint) propagates errors.
- Common mistake: expecting webhook headers to be validated/saved. Invalid header names/values are skipped at send time without an error.
- Common mistake: expecting the digest to list every downloaded video. Only fully successful ones are counted, and the list collapses to a summary above 10.
- Common mistake: expecting the Telegram image to be a local file. `sendPhoto` takes the remote `image_url` the server passes through (for the digest it is the first successful video's cover).
- Common mistake: expecting SMTP to always send multipart. Without an `image_url` it is a plain-text single part, and the subject is fixed.
- Common mistake: assuming the test endpoint and production sends share rendering behavior. The test path sets `ignore_cache`, so it always renders the freshly submitted template, while production uses the template registered in `TEMPLATE` from the saved config.

## How to verify

- Manual: add a webhook notifier in the settings UI and click test (`POST /api/config/notifiers/ping`); confirm the receiver gets the test body and that a wrong URL surfaces an error in the UI (the ping endpoint returns it).
- Manual: enable a Telegram notifier with `skip_image` toggled and compare `sendPhoto` vs `sendMessage`; the digest message starts with `<source>的 N 条新视频已入库`.
- Manual: configure SMTP with `None`, `Tls` and `StartTls` against a local mail catcher and verify subject `BiliSync 通知`, inline image presence, and that credentials are only used when filled.
- Manual: sync a source with more than 10 new videos and confirm the message switches from a numbered list to `… 条新视频已入库，快去看看吧！`.
- Manual: trigger a `-503` error with `ignore_common_errors` enabled and confirm only the log line appears and no notification is sent; disable it and confirm the notification arrives.
- Existing checks: `cargo test -p bili_sync`, `cargo clippy -- -D warnings`, `cargo +nightly fmt --check` (there is no dedicated notifier unit test in-tree).
