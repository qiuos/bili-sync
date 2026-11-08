# Media artifacts

## Responsibility and boundaries

- Owns: the on-disk layout produced for every synced video — directory derivation from templates, file names for video/episode/poster/fanart/NFO/danmaku/subtitles, the upper (person) directory, NFO XML content, ASS danmaku rendering, subtitle file naming, and the temporary-file/merge/parallel-download strategy of `Downloader`.
- Does not own: which videos are processed and when ([sync-pipeline.md](sync-pipeline.md)); template syntax and config validation ([config-and-persistence.md](config-and-persistence.md)); the bit-packed status fields that drive which artifacts are attempted ([sync-pipeline.md](sync-pipeline.md)).
- Talks to: `TEMPLATE`/`PathSafeTemplate` for path rendering, `utils::nfo::NFO`/`ToNFO` for XML, `BiliClient` for upstream URLs, `Downloader` for bytes, and the `Config` sub-structs `skip_option`, `nfo_time_type`, `time_format`, `upper_path`, `cdn_sorting`, `danmaku_option`.

## Sources of truth

- Download layout: `crates/bili_sync/src/workflow.rs` `download_video_pages` and `download_page`.
- Organization output: `crates/bili_sync/src/organization.rs` plans a separate hard-link library from existing download paths; original videos are not moved.
- Authoritative NFO schema: `crates/bili_sync/src/utils/nfo.rs` (`NFO::{Movie,TVShow,Upper,Episode}`, `ToNFO`, and the snapshot unit test). Do not hand-write XML elsewhere.
- Authoritative path rendering: `PathSafeTemplate::path_safe_render` in `crates/bili_sync/src/config/item.rs` plus `crates/bili_sync/src/config/handlebar.rs` (`video_name`, `page_name`, `*_default_path` templates).
- Authoritative danmaku output: `crates/bili_sync/src/bilibili/danmaku/writer.rs` (`DanmakuWriter::write`) and `ass_writer.rs`.
- Keep download naming in `workflow.rs`, organization naming in `organization.rs`, and NFO serialization in `utils/nfo.rs`.

## Main paths / change loci

- Key entry points:
  - `crates/bili_sync/src/workflow.rs`: `download_video_pages`, `download_page`, `fetch_video_poster`, `fetch_page_poster`, `fetch_page_video`, `generate_video_nfo`, `generate_page_nfo`, `fetch_upper_face`, `generate_upper_nfo`, `fetch_page_danmaku`, `fetch_page_subtitle`, `generate_nfo`.
  - `crates/bili_sync/src/downloader.rs`: `Downloader::{fetch,multi_fetch,multi_fetch_and_merge}`.
  - `crates/bili_sync/src/utils/filenamify.rs`: `filenamify` (character sanitizing applied to every rendered path segment).
- Common edit locations:
  - Rename or add an artifact: the tuple construction in `download_page` and the parallel `tokio::join!` right below it.
  - Change NFO content: `write_*_nfo` in `nfo.rs`, then update the snapshot test in the same file.
  - Change danmaku look: `DanmakuOption` defaults in `bilibili/danmaku/canvas/mod.rs`, lane/collision logic in `canvas/lane.rs`, ASS serialization in `ass_writer.rs`.
  - Change download strategy: `fetch_serial` / `fetch_parallel` / `multi_fetch_internal` in `downloader.rs`.

## Runtime paths and state transitions

- Trigger / inputs / outputs / interfaces:
  - Inputs: `video::Model` (name, bvid, cover, tags, single_page, `path`), `page::Model` (name, pid, cid, duration, width/height, `path`), the source `path`, rendered templates, and `skip_option`.
  - Outputs: files under the source directory; `video.path` is always persisted as the resolved base directory (single-page and multi-page alike) and `page.path` as the final `*.mp4` — both are the resume anchors.
  - Interface: `Downloader` accepts `&ConcurrentDownloadLimit` (`enable`, `concurrency`, `threshold` in bytes) and writes via `async_tempfile::TempFile` before copying into place.
- Layout, as implemented:
  - Video directory: `video_source.path()/video_name_template` on first download, or the stored `video.path`; it is persisted for both single-page and multi-page videos.
  - Upper directory: `config.upper_path/<first character of mid>/<mid>/`, containing `folder.jpg` and `person.nfo`. Only uppers in `upper_uids` are fetched, and only the first video of each upper in a run produces them.
  - Poster source: a single-page video uses `video.cover`; a multi-page episode uses `page.image` and falls back to `video.cover` when missing. The video-level `poster.jpg` always uses `video.cover`.
  - Single-page video (`single_page == true`), under the page directory:
    - `{base_name}-poster.jpg`, `{base_name}-fanart.jpg` (a copy of the poster),
    - `{base_name}.mp4`,
    - `{base_name}.nfo` (a `<movie>` NFO built from the video),
    - `{base_name}.zh-CN.default.ass` (danmaku),
    - `{base_name}.{lan}.srt` per subtitle language (`subtitle_path.with_extension("{lan}.srt")`).
  - Multi-page video:
    - page-level under `{video_dir}/Season 1/`: `{base_name} - S01E{pid:02}-thumb.jpg`, `{base_name} - S01E{pid:02}.mp4`, `{base_name} - S01E{pid:02}.nfo` (an `<episodedetails>` with `<season>1</season>` and `<episode>{pid}</episode>`), `{base_name} - S01E{pid:02}.zh-CN.default.ass`, and `{base_name} - S01E{pid:02}.{lan}.srt` per subtitle language. No per-episode fanart.
    - video-level under `{video_dir}/`: `poster.jpg` and `fanart.jpg` (both from the video cover; fanart is a copy), `tvshow.nfo` (a `<tvshow>` NFO).
  - `base_name` comes from the `page_name` handlebars template rendered by `PathSafeTemplate::path_safe_render`, so it may contain separators produced by the `__SEP__` substitution.
- Resume behavior: when `page.path` is non-empty it is parsed rather than re-rendered — single-page strips the `.mp4` suffix from the file name, multi-page takes the `Season 1` parent's parent as the base and splits the file name at `" - "`. A malformed stored path is an error (`invalid page path format`), never a silent re-render.
- `generate_nfo` creates the parent directory before writing, so an NFO can succeed even if the page directory was created moments earlier by a concurrent task.
- `Downloader` behavior:
  - `fetch`/`multi_fetch` stream to a temp file, then stage a unique sibling and rename it to the destination. `multi_fetch` retries URLs in order. Page video replacement also updates recorded hard links whose inode matches the previous source, under the organization lock. Unknown targets are not overwritten. Multi-file replacement uses best-effort rollback and is not crash-atomic.
  - `multi_fetch_and_merge` calls `ffmpeg -c copy -strict unofficial -f mp4 -y` (binary from `ARGS.ffmpeg_path`, default `ffmpeg`) to mux separate video and audio streams.
  - `fetch_serial` verifies the received byte count against `Content-Length` (`downloaded bytes mismatch: expected {}, got {}`).
  - `fetch_parallel` probes with `Range: bytes=0-0` GET instead of HEAD for streams (HEAD may 404 while GET works), uses HEAD + `Accept-Ranges` for plain files, falls back to serial, and verifies each chunk (`content length mismatch`, `downloaded bytes mismatch`).

## Organization output

- Rules use separate source and output paths. TV rules group all videos in the source directory, across uploaders, into one series named after the source directory's last path segment, use Season 1 and persist episode numbers by `(bvid, cid)`; new pages — including pages whose persisted record names a different legacy series — are appended after the historical maximum. Such legacy pages are planned as safe reorganisation items: before writing any artifact the execution persists a reorganisation intent record (`bvid` prefixed `reorg=`, same key index space, `path` = declared new mp4 target) that grants recoverable ownership of the target stems; artifacts are then written fill-missing-only (no overwrite inside the reorganisation target), and only after all files succeeded a single transaction updates the legacy record (`series`/`episode`/`path`) and deletes the intent row. On failure or interruption the intent row survives, the legacy record and legacy directory stay untouched, and a rerun resumes under the declared target; a declared path that no longer matches the computed plan conflicts instead of silently redirecting. Legacy STRM records (including under the legacy series name) still abort the whole group and are never migrated. Movie rules require explicit video selection for execution (the caller passes preview-derived IDs; execution revalidates) and treat multi-page videos as versions, each version NFO carrying its own page name.
- Video outputs are hard links to downloaded video files. NFO files use the existing serializer; available artwork, subtitles and ASS files are copied alongside the video. Before any write, an entry's MP4 target and all overwrite-class artifacts (per-episode NFO, series/movie `tvshow.nfo`/poster/fanart extras, per-episode `-thumb.jpg`/`-poster.jpg`) are pre-checked: a target mp4 occupied by an unowned file, or any pre-existing overwrite-class artifact on an entry without a persisted record of this rule declaring exactly that path, makes the entry a conservative conflict — no record is created and no user file is touched. Overwrite-class files are only replaced when this rule has a real persisted record whose stored path equals the entry's target, through a unique temp file plus atomic `rename` (which refuses to replace a symlink); a target whose path matches another rule's persisted record path is always skipped. During TV reorganisation the safety rules tighten further: a reorganisation item finalises (record migrated to the new series) only when every overwrite-class artifact in its plan is absent — any pre-existing NFO or cover next to the declared target is ownership-unprovable (user file vs. interrupted-run leftover) and forces a conservative conflict that keeps the legacy record and directory untouched, reported with an explicit message; the same rule blocks any execution-side replacement of the target mp4 between planning and writing, which is revalidated as same-source (`dev`+`ino`) of the source video; and in a directory that still has any unfinished reorganisation item (conflicted, failed, or succeeded this run), the directory-level extras (`poster`/`fanart`/`tvshow.nfo`) are fill-missing-only — pre-existing extras are never overwritten by ownership assumptions. Outside reorganisation, video hard links, subtitles and ASS danmaku are only filled when missing and never overwritten. An episode (or movie version) with no source artwork gets no borrowed artwork and the execution report carries an explicit missing-artwork message. Legacy STRM records cause a conflict and are not migrated or deleted.
- Known limitation: a new item may record its target before all artifacts are written. If execution fails or is interrupted and another process creates an NFO at that target before a retry, the retry can treat the record as ownership and overwrite that NFO. Back up output data and avoid external writes during organization and recovery.
- Hard links require a compatible filesystem shared by source and destination; cross-filesystem or unsupported operations fail without copying the video. Both names refer to the same data, so modifying either file changes the other; removing one link leaves the other intact. Validation rejects overlapping rule paths. Path checks and writes are not atomic with respect to external directory or mount changes: operators must prevent other processes from moving or replacing output directories, their ancestors, or mounts during execution.

## Constraints and failures

- Invariants:
  - File names are always produced by `PathSafeTemplate`/`filenamify`; never splice raw titles into paths.
  - `page.path` must end in `.mp4`; resume logic depends on it.
  - A page's `fanart` value is `None` for multi-page videos by design; adding per-episode fanart requires changing both the tuple and the video-level poster task.
  - NFO files must remain valid Emby/Jellyfin XML with the tags exercised by the snapshot test; adding fields means updating that test.
  - `skip_option` (`no_poster`, `no_video_nfo`, `no_upper`, `no_danmaku`, `no_subtitle`) turns the corresponding task into `ExecutionStatus::Skipped`, which still counts as success in the status bits.
- Failure modes and recovery:
  - Partial artifacts are safe: each subtask has its own status bits, so a failed subtitle or danmaku is retried without re-downloading the video.
  - A failed `canonicalize` (`canonicalize video path failed` / `canonicalize base path failed`) aborts that video/page; usually a missing or unreachable directory.
  - Mismatched byte counts abort before replacement; a failed staging copy preserves the destination. A crash during multi-link replacement can leave links on different versions; inspect the paths before retrying.
  - ffmpeg missing or failing is fatal at startup for the binary check and a per-page failure at merge time (`ffmpeg error: {stderr}`).
  - Deleting local files does not change download statuses or delete video records. The video redownload endpoint resets statuses and retries without deleting the stored paths or page rows.
- Compatibility / migration behavior:
  - Changing `video_name`/`page_name` templates only affects videos that have no stored path yet; already-downloaded rows keep their original paths. The API has no bulk path rewrite.
  - `nfo_time_type` (`FavTime`/`PubTime`) changes `<premiered>`/`<year>` for future NFO writes only.

## Forbidden patterns and common mistakes

- Common mistake: assuming replacement of multiple existing hard links is a single atomic filesystem transaction. It is not.
- Forbidden: guessing the artifact suffixes from the Emby docs instead of reading `download_page`; the project uses `-poster.jpg`/`-fanart.jpg`/`-thumb.jpg` (not `poster.jpg` per episode) and `.zh-CN.default.ass` (not `.ass`).
- Common mistake: expecting a `fanart.jpg` next to each episode of a multi-page video. Only `poster.jpg`/`fanart.jpg` exist at the video level.
- Common mistake: expecting `upper_path` to be a flat directory. It is `upper_path/<first digit of mid>/<mid>/`, and `Config::check` rejects a non-absolute `upper_path`.
- Common mistake: assuming the plain `{base_name}.srt` file exists. `fetch_page_subtitle` calls `subtitle_path.with_extension("{lan}.srt")`, so only the per-language files are written; a page with no subtitles produces none.
- Common mistake: assuming the danmaku subtask rewrites `danmaku_last_synced_at` on every run; only `ExecutionStatus::Succeeded` does, and only after `DanmakuWriter::write` returned.
- Common mistake: assuming `Downloader::fetch` uses parallel chunks for everything. Parallel mode requires `enable`, a size above `threshold`, and server support; otherwise it falls back to serial.
- Common mistake: assuming subtitles are always written with BOM/UTF-8 assumptions — the code writes `subtitle.body.to_string()` verbatim.

## How to verify

- NFO snapshots (exact XML for movie, tvshow, person, episodedetails): `cargo test -p bili_sync nfo`.
- Filename sanitizing: `cargo test -p bili_sync filenamify`.
- Template rendering (asserts `关注/永雏塔菲喵` → `关注_永雏塔菲/test/a`): `cargo test -p bili_sync handlebar`.
- ASS generation: `cargo test -p bili_sync ass_writer`.
- Manual check: sync one single-page and one multi-page video into a scratch source path, then run `find <path> -type f | sort` and compare against the two layouts above; confirm `page.path` in the database equals the `.mp4` file.
- Manual check: delete one artifact (for example the `.srt`), reset only that subtask through `POST /api/videos/{id}/update-status`, rerun, and confirm only the missing file is recreated.
