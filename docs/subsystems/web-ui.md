# Web UI

## Responsibility and boundaries

- Owns: the SvelteKit single-page admin app under `web/` — routes, navigation, the API client, the WebSocket client, status/segment rendering, filter state, settings forms (including notifier editing, Bilibili QR login, danmaku milestones) and the build artifacts embedded into the binary.
- Does not own: endpoint semantics and JSON contracts ([web-api-server.md](web-api-server.md)); server-side rules for status bits ([sync-pipeline.md](sync-pipeline.md)); configuration validation ([config-and-persistence.md](config-and-persistence.md)); notification delivery ([notifications.md](notifications.md)).
- Talks to: `/api` over `fetch` and `/api/ws` over a native `WebSocket`; persists only the auth token in `localStorage['authToken']`.

## Sources of truth

- Authoritative routes and screens: `web/src/routes/` (`+layout.svelte`, `+layout.ts`, `+page.svelte`, `logs/`, `videos/`, `video/[id]/`, `video-sources/`, `settings/`, `me/favorites`, `me/collections`, `me/uppers`).
- Authoritative navigation: `web/src/lib/components/app-sidebar.svelte` (`navMain`, `footer`).
- Authoritative API surface: `web/src/lib/api.ts` (`ApiClient`, `apiClient`, the `api` convenience object) and `web/src/lib/types.ts`.
- Authoritative socket protocol: `web/src/lib/ws.ts` (`EventType`, `WebSocketManager`, `wsManager`).
- Authoritative filter state: `web/src/lib/stores/filter.ts` (`AppState`, `appStateStore`, `ToQuery`, `ToFilterParams`).
- Authoritative status labels: `web/src/lib/components/status-editor.svelte` (`videoTaskNames`, `pageTaskNames`) and `web/src/lib/components/video-card.svelte` (`getStatusText`, `getOverallStatus`).
- Forbidden duplicate sources: a second fetch wrapper, hand-written endpoint strings outside `api.ts`, or status label lists diverging between `status-editor.svelte` and `video-card.svelte`.

## Main paths / change loci

- Key entry points:
  - `web/src/routes/+layout.ts` (`ssr = false`, `prerender = false`) and `web/src/routes/+layout.svelte` (sidebar + header + `#main` scroll container).
  - `web/src/lib/api.ts` (`ApiClient.request`, `setAuthToken`, `getAuthToken`, `clearAuthToken`, the `subscribeTo*` helpers).
  - `web/src/lib/ws.ts` (`WebSocketManager.connect`, `subscribe`, `scheduleReconnect`, `disconnect`).
- Common edit locations:
  - New screen: add a route directory under `web/src/routes/` and a sidebar entry in `app-sidebar.svelte`.
  - New API call: method on `ApiClient`, re-export in the `api` object, type in `web/src/lib/types.ts`.
  - New event type: `EventType` in `ws.ts`, plus the server side in `api/routes/ws/mod.rs`.
  - Status semantics/labels: `status-editor.svelte` + `video-card.svelte` together with `utils/status.rs`.
  - Dev proxy target: `web/vite.config.ts`.
- Routes present:
  - `/` dashboard: `getDashboard()` counts plus `subscribeToSysInfo`/`subscribeToTasks`; CPU and memory history arrays; disk usage computed as `(total_disk - available_disk) / total_disk`; triggers `POST /api/task/download` via `triggerDownloadTask`.
  - `/logs`: subscribes to logs and keeps the last 500 entries (`logs.slice(-499)`), auto-scrolls unless the user scrolled up.
  - `/videos`: list with URL-synced filters (page size 20), source/query/status/validation/created-time filters, record deletion and confirmed cleanup preview. Covers and titles open details; a cover-corner icon displays status.
  - `/organization`: create and edit TV or movie rules, browse server directories or enter paths, preview the plan, select movies and execute; results show item status and counts.
  - `/video/[id]`: detail, per-subtask status editor, reset (optional `force`), clear-and-reset (deletes pages and local directory).
  - `/video-sources`: tabs for favorites/collections/submissions/watch_later; add/update/remove/evaluate/full-sync; new items are created disabled.
  - `/settings`: token authentication bar, config tabs (basic/auth/filter/danmaku/notifiers/advanced), notifier CRUD + test, QR login dialog.
  - `/me/favorites`, `/me/collections`, `/me/uppers`: followed/subscribed lists, with 1-based `page_num` for collections and uppers.

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Build: `vite build` produces `web/build`, which `task/http_server.rs` embeds via `#[folder = "../../web/build"]`. The app is fully client-side (`ssr = false`, `prerender = false`, adapter-static with `fallback: 'index.html'`).
  - Runtime inputs: HTTP JSON from `/api`, WebSocket events, `localStorage['authToken']`.
  - Runtime outputs: rendered screens, toasts (`svelte-sonner`), mutations through the API.
- Main execution path (source files and named symbols):
  1. `ApiClient` is constructed at module load with `API_BASE_URL = '/api'`, `Content-Type: application/json`, and `Authorization` taken from `localStorage['authToken']`. `request()` serializes defined query parameters, JSON-encodes non-GET bodies, and maps any non-2xx response to `{ message, status }` by reading `message`/`error` from the JSON body.
  2. `setAuthToken` stores the token and header; `clearAuthToken` deletes both and calls `wsManager.disconnect()`. Authentication is entered in `/settings` (the "前端认证状态" bar), which then reloads the config to prove the token works.
  3. `WebSocketManager` is a singleton. `connect()` builds `${ws|wss}://${location.host}/api/ws` and passes the token as the subprotocol: `btoa(token)` with `+`→`-`, `/`→`_`, padding stripped (matching the server's `BASE64_URL_SAFE_NO_PAD`). On open it re-subscribes every event in `subscribedEvents`.
  4. Subscription is reference-counted by callback sets: the first subscriber for an event sends `{"subscribe": "<event>"}`, the last unsubscriber sends `{"unsubscribe": "<event>"}`. Incoming frames are parsed as `{ logs?, tasks?, sysInfo? }` and fanned out to the matching callback set; a parse failure shows a toast.
  5. Reconnection: on `close`, `scheduleReconnect` retries up to `maxReconnectAttempts = 5` with `1000 * 2^n` ms delays; after that it stops silently (`console.log('Max reconnect attempts reached')`).
  6. Filter state lives in `appStateStore` (defaults `validationFilter: 'normal'`, `currentPage: 0`); `ToQuery` maps it to URL parameters (`page`, `query`, `<sourceType>`, `status_filter`, `validation_filter`, `created_from`, `created_to`) and `ToFilterParams` to API parameters, so filtered views are shareable/reloadable.
  7. Status rendering: the API returns `download_status` as five numbers. `getStatusText` renders `7` → 已完成, `0` → 未开始, else 失败N次; `getOverallStatus` prioritizes `!valid` → 失效, `!should_download` → 跳过, all-7 → 完成, any-not-7-not-0 → 失败, else 等待. The editor only allows setting 0 or 7, and resetting page status forces the video's index 4 ("分页下载") to 0 on the server.
  8. Video cards load downloaded covers through an authenticated fetch and reference-counted object URLs (`video-cover.svelte`, `cover-cache.ts`); missing covers show a placeholder. Organization dialogs use the rule and preview APIs.
  9. Settings forms: `intervalInput` is parsed on save — a purely numeric string becomes a number (`Trigger::Interval`), anything else a string (`Trigger::Cron`). Notifier dialog fields mirror `Notifier` (telegram/webhook/smtp); SMTP defaults to port 465 and `tls`. The QR dialog polls `GET /api/login/qrcode/poll` every 2 seconds with a 180-second expiry.
- State ownership, transitions and persistence:
  - The only persisted client state is `localStorage['authToken']`; everything else is in-memory and lost on reload.
  - The socket's `subscribedEvents` set is cleared by `disconnect()` and replayed after reconnect.
  - No optimistic updates for the config: `updateConfig` replaces `formData` with the server response, so a rejected save leaves the previous values visible.

## Constraints and failures

- Invariants:
  - JSON requests go through `api.ts`; it owns the base URL, token header and error normalization. Local cover image fetches use the same authentication token via the cover cache.
  - The socket URL must stay on the same origin (`/api/ws`) so the embedded build works behind any reverse proxy; only the protocol follows `location.protocol`.
  - `download_status` is always treated as a five-element array; never as a bitfield in the UI.
  - Task labels must match the backend offsets: video `['视频封面','视频信息','UP 主头像','UP 主信息','分页下载']`, page `['视频封面','视频内容','视频信息','视频弹幕','视频字幕']`.
- Failure modes and recovery:
  - 401 from any call surfaces as `{message: 'auth token does not match', status: 401}`; the user re-enters the token in `/settings`. There is no global interceptor — each page shows a toast.
  - A lost WebSocket degrades to at most 5 exponential retries; afterwards log/task/system-info panels stop updating until reload.
  - A failed save (validation or version mismatch) shows the server `message` and keeps the form values; the user must reload to pick up `version` changes.
  - `clear-and-reset-status` may return a `warning` string when the local directory cannot be removed; the operation still succeeds in the database.
- Compatibility / migration behavior:
  - The client must be rebuilt and re-embedded for any API change; `web/build` is baked into the binary at compile time (the `build-frontend` recipe in `Justfile` runs `bun run build`).
  - `web/src/lib/types.ts` declares `SysInfo.used_disk`, which the server does not send; the dashboard computes disk usage from `total_disk`/`available_disk`, so the extra field is unused. Do not rely on it.
  - Dev mode proxies `/api/ws` (WebSocket) and `/api` to `http://localhost:12345` (`vite.config.ts`), so a backend must be running for `bun run dev`.

## Forbidden patterns and common mistakes

- Forbidden: hard-coding the backend host in the client; production is served from the same origin as the embedded assets.
- Forbidden: putting the token in a query string or URL. It is sent as `Authorization` for HTTP and as a WebSocket subprotocol.
- Forbidden: clearing `localStorage` directly instead of calling `clearAuthToken()`; the socket would stay authenticated.
- Common mistake: assuming the UI is server-rendered. `ssr = false` and `prerender = false` mean every screen is client-only and `onMount` is where subscriptions belong.
- Common mistake: assuming `currentPage` is 1-based. The videos store starts at `0`; the `me/*` pages pass `page + 1` because those Bilibili-backed endpoints are 1-based.
- Common mistake: assuming a status value is a retry count. Values are `0` (not started), `1..3` (failures so far), `4` (retries exhausted, terminal failure), `7` (done); the UI shows 4 as 失败4次.
- Common mistake: expecting `/video-sources` new subscriptions to be active immediately. Inserts are stored with `enabled: false`; the user must enable them.
- Common mistake: expecting the settings page to validate cron locally. It only distinguishes numeric vs string; the server rejects invalid expressions with the Chinese message from `Config::check`.
- Common mistake: expecting the notifier test to use the saved template. The ping request carries the current form notifier and the server forces a fresh render.

## How to verify

- Type/lint/build (from `web/`): `bun run check` (svelte-check), `bun run lint` (`prettier --check . && eslint .`), `bun run build`; the backend embeds `web/build`, so a successful binary build implies the directory exists.
- Dev check: run the backend on `12345`, then `bun run dev`; confirm the proxy serves `http://localhost:5173` and that `/api/ws` connects using the token entered in `/settings`.
- Auth check: open the app with no token → pages show 401 toasts; enter the token in `/settings` → config loads; click 退出认证 → token removed and the socket disconnects.
- Live data check: open `/`, confirm CPU/memory charts update roughly every 2 seconds and the task card reflects a manual `POST /api/task/download`; open `/logs` and confirm the last 200 historical lines appear immediately, then live lines follow.
- Filter check: apply status/validation/created-time filters on `/videos`, copy the URL, reopen it and confirm the same filtered view is restored.
- Status editing check: in `/video/[id]` set a subtask to 已完成 or 未开始, save, reload and confirm the five-segment bar matches; run 重置 and confirm failed segments return to not-started.
