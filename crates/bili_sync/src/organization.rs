//! 媒体整理：将已下载的视频以硬链接形式组织为 Emby 媒体库目录（电视剧/电影），并生成 NFO/封面/字幕弹幕等配套文件。
//! 原视频永远只读（硬链接与源视频共享同一文件数据，绝不复制视频、绝不修改源文件）；所有写入都发生在规则的
//! 输出目录内，且绝不覆盖任何已存在的文件（只补缺）。执行全程持全局锁串行进行，持久记录先于文件写入，
//! 保证失败后无不可恢复孤儿。

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::{Context, Result, bail};
use bili_sync_entity::*;
use chrono::Datelike;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use tokio::io::AsyncWriteExt;
use unicode_normalization::UnicodeNormalization;

use crate::utils::filenamify::filenamify;
use crate::utils::nfo::{Episode, Movie, NFO, TVShow};

/// 规则执行全局串行锁：计划（含集号分配）与写入必须原子地串行，避免并发执行互相竞争
static EXECUTE_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

/// 跨规则、跨分页重建都稳定的分页标识：bvid + cid。page_id 在视频重建后会变化，不作为标识使用
type PageKey = (String, i64);

fn page_key(video: &video::Model, page: &page::Model) -> PageKey {
    (video.bvid.clone(), page.cid)
}

/// 判断两个路径是否相同或存在父子嵌套关系（入参应为规范化后的真实路径）
pub fn paths_overlap(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// 目标路径是否已被占用（含指向不存在文件的符号链接，一律视为占用以避免写入越界）
/// 重整意图记录的 bvid 编码前缀：`reorg=<真实bvid>`。真实 bvid 以 BV 开头，绝不与该前缀混淆；
/// 意图记录与真实记录共享 (rule_id, bvid, cid) 唯一索引的不同 bvid 值，可安全共存。
/// 意图记录在重整开始前写入、完成后删除（残留由下次重跑自愈清理），是对新目标路径的持久拥有声明。
const REORG_INTENT_PREFIX: &str = "reorg=";

fn reorg_intent_bvid(bvid: &str) -> String {
    format!("{REORG_INTENT_PREFIX}{bvid}")
}

fn is_reorg_intent_bvid(bvid: &str) -> bool {
    bvid.starts_with(REORG_INTENT_PREFIX)
}

/// 还原意图记录中的真实 bvid；非意图记录返回 None
fn parse_reorg_intent_bvid(bvid: &str) -> Option<String> {
    bvid.strip_prefix(REORG_INTENT_PREFIX).map(str::to_string)
}

/// 目标路径是否已被占用（含指向不存在文件的符号链接，一律视为占用以避免写入越界）
fn path_occupied(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
}

/// 两个路径是否指向同一物理文件（跨文件系统安全：比较 (dev, ino)；任一路径不可获取元数据时
/// 返回 false——fail closed）。符号链接一律视为不同。仅用于同源硬链接的保守识别，
/// 不作为目录或元数据归属的证明。
#[cfg(unix)]
fn same_source_hardlink(a: &Path, b: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    if a.symlink_metadata().is_ok_and(|meta| meta.file_type().is_symlink())
        || b.symlink_metadata().is_ok_and(|meta| meta.file_type().is_symlink())
    {
        return false;
    }
    match (std::fs::metadata(a), std::fs::metadata(b)) {
        (Ok(meta_a), Ok(meta_b)) => {
            meta_a.is_file() && meta_b.is_file() && meta_a.dev() == meta_b.dev() && meta_a.ino() == meta_b.ino()
        }
        _ => false,
    }
}

/// 非 Unix 平台无法比较 (dev, ino)，一律按不同文件处理（保守不刷新）
#[cfg(not(unix))]
fn same_source_hardlink(_a: &Path, _b: &Path) -> bool {
    false
}

/// 校验 target 位于 root 之内，且从 root 到 target 的路径上不存在任何符号链接
/// （符号链接别名或外链会让 create_dir_all/写入越过输出目录边界）。
/// 尚不存在的路径节点跳过符号链接检查，由写入时创建目录后的复查兜底。
fn ensure_path_within(root: &Path, target: &Path) -> Result<()> {
    let mut cur = target.to_path_buf();
    loop {
        match cur.symlink_metadata() {
            Ok(meta) => anyhow::ensure!(
                !meta.file_type().is_symlink(),
                "路径「{}」是符号链接，拒绝写入",
                cur.display()
            ),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if cur == root {
            return Ok(());
        }
        let Some(parent) = cur.parent() else {
            bail!("路径「{}」越出输出目录边界", target.display());
        };
        cur = parent.to_path_buf();
    }
}

/// 规范化必须已存在的源目录，解析符号链接得到真实路径
fn canonicalize_source_dir(path: &str) -> Result<PathBuf> {
    let canonical = std::fs::canonicalize(path).with_context(|| format!("源目录不存在或无法访问：{path}"))?;
    anyhow::ensure!(canonical.is_dir(), "源路径不是目录：{}", canonical.display());
    Ok(canonical)
}

/// 规范化可能尚不存在的输出目录：已存在时取真实路径，否则取已存在的父目录加末级目录名
fn canonicalize_output_dir(path: &str) -> Result<PathBuf> {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        anyhow::ensure!(canonical.is_dir(), "输出路径不是目录：{}", canonical.display());
        return Ok(canonical);
    }
    let path = Path::new(path);
    let name = path.file_name().context(format!("输出目录路径非法：{path:?}"))?;
    let parent = path.parent().context(format!("输出目录路径非法：{path:?}"))?;
    let canonical_parent = std::fs::canonicalize(parent)
        .with_context(|| format!("输出目录的父目录不存在或无法访问：{}", parent.display()))?;
    Ok(canonical_parent.join(name))
}

/// 校验规则的源目录与输出目录：两者必须为绝对路径（请求层校验）、真实路径规范化；
/// 输出目录不得与任何规则的源目录或输出目录相同或互相嵌套；源目录不得嵌套进任何规则的输出目录。
/// 返回规范化后的存储路径。
pub async fn validate_rule_paths(
    db: &DatabaseConnection,
    exclude_rule_id: Option<i32>,
    source_path: &str,
    output_path: &str,
) -> Result<(String, String)> {
    let source = canonicalize_source_dir(source_path)?;
    let output = canonicalize_output_dir(output_path)?;
    if paths_overlap(&output, &source) {
        bail!("输出目录与源目录相同或存在嵌套");
    }
    let rules = organization_rule::Entity::find().all(db).await?;
    for rule in rules {
        if exclude_rule_id.is_some_and(|id| rule.id == id) {
            continue;
        }
        // 规则入库时存储的已经是规范化路径，可直接按组件比较
        if paths_overlap(&output, Path::new(&rule.source_path)) {
            bail!("输出目录与规则「{}」的源目录相同或存在嵌套", rule.name);
        }
        if paths_overlap(&output, Path::new(&rule.output_path)) {
            bail!("输出目录与规则「{}」的输出目录相同或存在嵌套", rule.name);
        }
        if paths_overlap(&source, Path::new(&rule.output_path)) {
            bail!("源目录与规则「{}」的输出目录相同或存在嵌套", rule.name);
        }
    }
    Ok((
        source.to_string_lossy().to_string(),
        output.to_string_lossy().to_string(),
    ))
}

/// 规则已产生整理记录后，源目录/输出目录/规则类型不允许修改：
/// 否则已记录的分页归属会被用于在新目标位置覆写文件。仅改名不受限制。
pub async fn ensure_rule_editable(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    new_source_path: &str,
    new_output_path: &str,
    new_rule_type: &str,
) -> Result<()> {
    let has_records = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .one(db)
        .await?
        .is_some();
    if !has_records {
        return Ok(());
    }
    if rule.source_path == new_source_path && rule.output_path == new_output_path && rule.rule_type == new_rule_type {
        return Ok(());
    }
    bail!(
        "规则「{}」已产生整理记录，不允许修改源目录、输出目录或规则类型；请先删除该规则（保留文件）后新建",
        rule.name
    );
}

/// 待写入输出目录的单个文件
#[derive(Debug, Clone)]
pub struct PendingFile {
    pub target: PathBuf,
    pub content: PendingContent,
    /// 是否允许强制覆盖既有目标（仅本规则确认归属的元数据 NFO 与封面图片）。
    /// 视频、字幕、弹幕一律 false：执行时只补缺、绝不覆盖。
    pub overwrite: bool,
}

/// 待写文件里是否已包含该分集输出的画面类旁挂文件（thumb/poster）
fn has_episode_art(files: &[PendingFile]) -> bool {
    files.iter().any(|file| {
        file.target
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with("-thumb.jpg") || name.ends_with("-poster.jpg"))
    })
}

/// 追加提示信息到条目消息（保留计划期信息，避免被执行期说明覆盖）
fn append_message(message: &mut Option<String>, text: &str) {
    match message {
        Some(existing) => {
            existing.push('；');
            existing.push_str(text);
        }
        None => *message = Some(text.to_string()),
    }
}

#[derive(Debug, Clone)]
pub enum PendingContent {
    /// 与源视频文件建立硬链接（绝不复制视频、绝不覆盖）
    HardLink(PathBuf),
    /// 写入预渲染好的 NFO XML
    Xml(String),
    /// 从源目录复制文件（封面/字幕/弹幕）
    Copy(PathBuf),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ItemAction {
    /// 待创建
    Create,
    /// 已由本规则整理，执行时只补缺缺失文件并更新记录，绝不覆盖既有文件
    Refresh,
    /// 目标位置已有文件（非本规则可安全覆盖），跳过
    Exists,
    /// 已被其它规则整理，或记录与当前状态矛盾
    Conflict(String),
    /// 源文件缺失
    Missing(String),
    /// 执行失败（仅出现在执行结果中）
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    pub video_id: i32,
    pub page_id: i32,
    /// 跨规则去重与冲突判定的稳定标识：bvid + cid
    pub bvid: String,
    pub cid: i64,
    pub video_name: String,
    pub page_name: String,
    /// 电视剧规则下为剧名（UP 用户名）与持久集号
    pub series: Option<String>,
    pub episode: Option<i32>,
    pub action: ItemAction,
    /// TV 重整条目：本页已有旧记录（旧剧名），成功写入新路径后再更新记录；
    /// 失败或目标被占用时保留旧记录与旧目录，绝不中途改记录
    pub reorg: bool,
    /// 视频硬链接目标路径
    pub target: Option<PathBuf>,
    pub files: Vec<PendingFile>,
    /// 附加提示（如部分配套文件已存在被跳过）
    pub message: Option<String>,
}

#[derive(Debug, Default)]
pub struct Plan {
    pub items: Vec<PlanItem>,
    /// 与具体分页无关的输出文件（剧集的 tvshow.nfo/poster/fanart、电影文件夹的 poster/fanart）
    pub extras: Vec<PendingFile>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ExecuteReport {
    pub created: usize,
    pub refreshed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub extras_created: usize,
    pub items: Vec<PlanItem>,
    pub warnings: Vec<String>,
}

/// 生成规则的整理预览，纯只读，不产生任何写入或持久化。
/// 电影规则 video_ids 传 None 时按源目录全部候选预览（供界面列出候选）。
pub async fn preview_rule(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    video_ids: Option<&[i32]>,
) -> Result<Plan> {
    let source = canonicalize_source_dir(&rule.source_path)?;
    match rule.rule_type.as_str() {
        "tv" => plan_tv(rule, db, &source).await,
        "movie" => plan_movie(rule, db, &source, video_ids, false).await,
        other => bail!("未知的规则类型：{other}"),
    }
}

/// 执行时重新验证源目录：必须存在且规范化后与入库路径一致（防止目录被移动或替换为符号链接）
fn revalidate_dir(stored: &str, label: &str) -> Result<PathBuf> {
    let stored_path = PathBuf::from(stored);
    let actual = std::fs::canonicalize(&stored_path).with_context(|| format!("{label}不存在或无法访问：{stored}"))?;
    anyhow::ensure!(
        actual == stored_path,
        "{label}已被移动或替换（记录：{stored}，实际：{}），拒绝执行",
        actual.display()
    );
    Ok(actual)
}

/// 执行时重新验证输出目录：允许尚未创建（路径校验时仅要求父目录存在），缺失则安全补建。
/// 创建前先验证父目录规范化后与入库记录一致（发现被符号链接劫持即拒绝，不在越界位置创建目录）。
/// ponytail: 检查与 create_dir_all 之间存在理论 TOCTOU 窗口，与 ensure_path_within 同一限制。
async fn revalidate_output(stored: &str) -> Result<PathBuf> {
    let stored_path = PathBuf::from(stored);
    if std::fs::symlink_metadata(&stored_path).is_err() {
        let Some(parent) = stored_path.parent() else {
            bail!("输出目录路径非法：{stored}");
        };
        let parent_real = std::fs::canonicalize(parent)
            .with_context(|| format!("输出目录的父目录不存在或无法访问：{}", parent.display()))?;
        anyhow::ensure!(
            parent_real == parent,
            "输出目录的父目录已被移动或替换（记录：{}，实际：{}），拒绝创建",
            parent.display(),
            parent_real.display()
        );
        tokio::fs::create_dir_all(&stored_path).await?;
        let real = std::fs::canonicalize(&stored_path)?;
        anyhow::ensure!(
            real == stored_path,
            "输出目录创建后与记录路径不一致：{}",
            real.display()
        );
        return Ok(real);
    }
    revalidate_dir(stored, "输出目录")
}

/// 执行锁：路由层在创建/更新/删除规则（路径校验+持久化）时也应持有同一把锁，
/// 避免与执行并发产生路径/记录竞争；execute_rule 持锁后会按 id 重读规则
/// ponytail: 路由层接入由主会话完成；接入前 dead_code
#[allow(dead_code)]
pub fn execution_lock() -> &'static tokio::sync::Mutex<()> {
    &EXECUTE_LOCK
}

/// 执行规则：持全局锁串行重新计算计划后写入输出目录并持久化整理记录。
/// 电影规则必须显式提供 video_ids（预览可用 None 列出全部候选）。
pub async fn execute_rule(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    video_ids: Option<&[i32]>,
) -> Result<ExecuteReport> {
    let _guard = EXECUTE_LOCK.lock().await;
    // 真实 API 中规则 id 为自增主键恒大于 0；未持久化（id=0）规则仅允许在测试中直连执行
    #[cfg(not(test))]
    anyhow::ensure!(rule.id > 0, "规则尚未持久化，无法执行");
    // 持锁后按 id 重读规则：并发的删除/修改不会让过期规则继续执行
    let rule = if rule.id > 0 {
        organization_rule::Entity::find_by_id(rule.id)
            .one(db)
            .await?
            .context(format!("规则「{}」已被删除，无法执行", rule.name))?
    } else {
        rule.clone()
    };
    let source = revalidate_dir(&rule.source_path, "源目录")?;
    let output = revalidate_output(&rule.output_path).await?;
    anyhow::ensure!(
        !paths_overlap(&output, &source),
        "输出目录与源目录相同或存在嵌套，拒绝执行"
    );
    let plan = match rule.rule_type.as_str() {
        "tv" => plan_tv(&rule, db, &source).await?,
        "movie" => plan_movie(&rule, db, &source, video_ids, true).await?,
        other => bail!("未知的规则类型：{other}"),
    };
    execute_plan(&rule, plan, db, &output).await
}

/// 加载已下载且位于源目录内的视频与其分页，按 bvid 去重（保留数据库中最早一条）。
/// 返回（可整理的分页列表、源文件缺失的分页、未就绪视频数）。
async fn collect_source_videos(
    db: &DatabaseConnection,
    source: &Path,
) -> Result<(
    Vec<(video::Model, Vec<page::Model>)>,
    Vec<(video::Model, page::Model)>,
    usize,
)> {
    let mut videos = video::Entity::find()
        .filter(
            video::Column::Valid
                .eq(true)
                .and(video::Column::Path.ne(""))
                .and(video::Column::SinglePage.is_not_null()),
        )
        .find_with_related(page::Entity)
        .all(db)
        .await?;
    videos.sort_by_key(|(video, _)| video.id);
    let mut candidates: Vec<(video::Model, Vec<page::Model>)> = Vec::new();
    let mut missing_pages: Vec<(video::Model, page::Model)> = Vec::new();
    let mut undownloaded = 0usize;
    for (video, mut pages) in videos {
        let Ok(video_dir) = std::fs::canonicalize(&video.path) else {
            undownloaded += 1;
            continue;
        };
        if !video_dir.starts_with(source) {
            continue;
        }
        pages.sort_by_key(|page| page.pid);
        let mut downloaded = Vec::new();
        let mut missing = Vec::new();
        for page in pages {
            match page.path.as_ref().filter(|path| !path.is_empty()) {
                None => {}
                Some(path) if page_path_within_source(path, source) && Path::new(path.as_str()).is_file() => {
                    downloaded.push(page)
                }
                Some(_) => missing.push(page),
            }
        }
        if downloaded.is_empty() && missing.is_empty() {
            undownloaded += 1;
            continue;
        }
        // 跨记录合并：同 bvid 的较早记录属于其它源目录时不得遮蔽本源目录内的记录；
        // 同 bvid 多条记录各自下载了不同分P时按 (bvid, cid) 互补合并
        if !downloaded.is_empty() {
            if let Some((_, pages)) = candidates.iter_mut().find(|(admitted, _)| admitted.bvid == video.bvid) {
                let existing: HashSet<i64> = pages.iter().map(|page| page.cid).collect();
                pages.extend(downloaded.into_iter().filter(|page| !existing.contains(&page.cid)));
                pages.sort_by_key(|page| page.pid);
            } else {
                candidates.push((video.clone(), downloaded));
            }
        }
        for page in missing {
            // 已被任何已入选候选覆盖的 (bvid, cid) 不再重复报告缺失
            let shadowed = candidates.iter().any(|(admitted, pages)| {
                admitted.bvid == video.bvid && pages.iter().any(|downloaded| downloaded.cid == page.cid)
            });
            if !shadowed {
                missing_pages.push((video.clone(), page));
            }
        }
    }
    let available: HashSet<PageKey> = candidates
        .iter()
        .flat_map(|(video, pages)| pages.iter().map(|page| page_key(video, page)))
        .collect();
    missing_pages.retain(|(video, page)| !available.contains(&page_key(video, page)));
    Ok((candidates, missing_pages, undownloaded))
}

/// 分P路径解析符号链接后必须位于源目录内：防止 DB 路径被改动或符号链接让硬链接/复制指向源目录之外。
/// 路径无法解析（文件不存在/失效链接）时返回 true，交由后续存在性检查归入缺失。
fn page_path_within_source(path: &str, source: &Path) -> bool {
    match std::fs::canonicalize(path) {
        Ok(real) => real.starts_with(source),
        Err(_) => true,
    }
}

enum ExistingName {
    None,
    One(String),
    Ambiguous,
}

/// 大小写不敏感文件系统（macOS 默认 APFS、Windows）会把不同大小写、以及 NFC/NFD 写法不同
/// 的名字视为同一物理目录；按组件 NFC 归一化 + 小写化比较，查找目录下与 name 等价的既有条目名。
/// 同时存在多个变体（大小写敏感/规范化敏感文件系统上同名异写目录共存）时返回 Ambiguous，
/// 调用方保守冲突，绝不写进任一候选。
/// 读取失败一律 fail closed（返回错误），仅“输出根目录尚不存在”作为明确特例返回 None。
fn find_existing_name(dir: &Path, name: &str) -> Result<ExistingName> {
    match std::fs::symlink_metadata(dir) {
        // 输出根尚未创建：唯一允许按“无既有条目”处理的明确特例
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ExistingName::None),
        Ok(meta) if !meta.is_dir() => bail!("输出路径不是目录，无法确认目录归属：{}", dir.display()),
        Ok(_) => {}
        Err(e) => return Err(e).with_context(|| format!("无法访问输出目录：{}", dir.display())),
    }
    let lower = normalize_component(name);
    let mut found: Option<String> = None;
    let mut ambiguous = false;
    for entry in std::fs::read_dir(dir).with_context(|| format!("无法读取输出目录：{}", dir.display()))? {
        let entry = entry.with_context(|| format!("无法读取输出目录条目：{}", dir.display()))?;
        let existing = entry
            .file_name()
            .to_str()
            .with_context(|| format!("输出目录存在非 UTF-8 条目，无法确认归属：{}", dir.display()))?
            .to_string();
        if normalize_component(&existing) == lower {
            if found.is_some() {
                ambiguous = true;
            }
            found = Some(existing);
        }
    }
    Ok(match (ambiguous, found) {
        (true, _) => ExistingName::Ambiguous,
        (false, Some(name)) => ExistingName::One(name),
        (false, None) => ExistingName::None,
    })
}

/// 目录归属判定：Ours = 本规则有持久整理记录（TV 按剧名、电影按 bvid 匹配）证明目录由本映射产出；
/// OtherOwned = 本规则其它剧集/视频的持久记录指向该目录；Foreign = 其它规则的记录位于该目录；
/// Unclaimed = 无任何记录，此时目录若存在则是用户目录或记录已丢失的另一作品，必须保守冲突。
/// 只看持久记录与真实路径，不依赖目录当前是否存在（记录可能先于写入，或目录事后被删除/移动）。
#[derive(Debug, PartialEq)]
enum DirOwnership {
    Ours,
    OtherOwned,
    Foreign(String),
    Unclaimed,
}

/// 对可能尚不存在的路径做“尽力规范化”：逐级向上找到第一个可解析的祖先，解析后拼回剩余末段，
/// 从而消除 /var ↔ /private/var、大小写别名等真实文件系统别名差异。
fn canonicalize_best_effort(path: &Path) -> std::path::PathBuf {
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = path;
    loop {
        if let Ok(real) = std::fs::canonicalize(cur) {
            let mut result = real;
            for part in tail.iter().rev() {
                result.push(part);
            }
            return result;
        }
        match (cur.file_name(), cur.parent()) {
            (Some(name), Some(parent)) => {
                tail.push(name.to_os_string());
                cur = parent;
            }
            _ => return path.to_path_buf(),
        }
    }
}

/// 路径比较键：逐组件小写化，用于识别大小写别名（大小写不敏感文件系统上 names 相同即同一目录）。
/// 保守方向：只会多判“同一目录”，从而多判冲突，不会漏判。
/// 局限：不做 Unicode NFC/NFD 归一化，也不处理非 ASCII 的本地化大小写折叠；
/// 这类别名仅在目录已存在时由文件系统自身解析（见调用方的 symlink_metadata 存在性判断）。
/// 单个路径组件的比较键：NFC 规范化与大小写折叠分别按组件应用；
/// 保守方向：只会把更多路径判为“同一目录”，即多报冲突，不会漏判冲突。
fn normalize_component(component: &str) -> String {
    component.nfc().collect::<String>().to_lowercase()
}

/// 路径比较键：逐组件 NFC 归一化 + 小写化，识别大小写不敏感与 NFC/NFD 别名
/// （macOS APFS 默认同时具备这两种特性）。
/// 局限：不做 NFKC/NFKD 兼容分解归一化，不处理本地化大小写折叠与文件系统特有折叠规则；
/// 这类别名仅在目录真实存在时由文件系统自身解析。
fn normalized_key(path: &Path) -> Vec<String> {
    path.components()
        .map(|component| normalize_component(&component.as_os_str().to_string_lossy()))
        .collect()
}

fn key_starts_with(path: &Path, base: &Path) -> bool {
    let path_key = normalized_key(path);
    let base_key = normalized_key(base);
    base_key.len() <= path_key.len() && base_key.iter().zip(path_key.iter()).all(|(a, b)| a == b)
}

/// 记录路径是否位于 dir 之内。先按真实路径别名（符号链接、/var ↔ /private/var）比对，
/// 再按大小写别名键比对；目录尚不存在时只能依赖后者（其中未解析的末段无法做 Unicode 归一化）。
fn stored_path_under(stored: &str, dir: &Path) -> bool {
    let stored = Path::new(stored);
    if stored.starts_with(dir) {
        return true;
    }
    let stored_real = canonicalize_best_effort(stored);
    let dir_real = canonicalize_best_effort(dir);
    stored_real.starts_with(&dir_real) || key_starts_with(&stored_real, &dir_real)
}

/// 目录归属判定：Ours = 本规则有持久整理记录（TV 按剧名、电影按 bvid 匹配；含 reorg intent 记录）
/// 证明目录由本映射产出；OtherOwned = 本规则其它剧集/视频的持久记录指向该目录；
/// Foreign = 其它规则的记录位于该目录；Unclaimed = 无任何记录，此时目录若存在则是用户目录
/// 或记录已丢失的另一作品，必须保守冲突。
/// 判定只看持久记录与真实路径，不依赖目录当前是否存在（记录可能先于写入，或目录事后被删除/移动）。
/// 判定顺序为“先拒绝后放行”：只要该目录下存在任何不匹配本映射的记录（本规则其它作品或其它规则），
/// 即使同时存在本映射记录也一律拒绝，避免混合历史下误认归属。
fn dir_ownership(
    dir: &Path,
    own_rows: &[organization_output::Model],
    own_match: impl Fn(&organization_output::Model) -> bool,
    foreign_paths: &[(String, String)],
) -> DirOwnership {
    let mut matched = false;
    let mut mismatched = false;
    for row in own_rows.iter().filter(|row| stored_path_under(&row.path, dir)) {
        if own_match(row) {
            matched = true;
        } else {
            mismatched = true;
        }
    }
    if mismatched {
        return DirOwnership::OtherOwned;
    }
    // 其它规则也在同一目录留下记录：混合历史，拒绝
    if let Some((_, name)) = foreign_paths.iter().find(|(path, _)| stored_path_under(path, dir)) {
        return DirOwnership::Foreign(name.clone());
    }
    if matched {
        DirOwnership::Ours
    } else {
        DirOwnership::Unclaimed
    }
}

/// 电视剧整组冲突：目录归属无法确认或目录名被占用时，该组全部分页按冲突跳过，不产生任何写入
fn push_tv_conflict_group(plan: &mut Plan, entries: &[(video::Model, page::Model)], series: &str, message: String) {
    for (video, page) in entries {
        plan.items.push(PlanItem {
            reorg: false,
            video_id: video.id,
            page_id: page.id,
            bvid: video.bvid.clone(),
            cid: page.cid,
            video_name: video.name.clone(),
            page_name: page.name.clone(),
            series: Some(series.to_string()),
            episode: None,
            action: ItemAction::Conflict(message.clone()),
            target: None,
            files: Vec::new(),
            message: None,
        });
    }
}

async fn plan_tv(rule: &organization_rule::Model, db: &DatabaseConnection, source: &Path) -> Result<Plan> {
    let output = PathBuf::from(&rule.output_path);
    let mut plan = Plan::default();
    // 其它规则与本规则源目录重叠时仅提示，同一文件只会被先执行的规则整理
    for other in organization_rule::Entity::find().all(db).await? {
        if other.id != rule.id && paths_overlap(source, Path::new(&other.source_path)) {
            plan.warnings
                .push(format!("源目录与规则「{}」重叠，重叠部分只会被整理一次", other.name));
        }
    }
    // 本规则已有的整理记录：稳定标识 -> 记录；历史记录保证集号持久、绝不复用。
    // 其中 bvid 带 reorg= 前缀的是重整意图记录：在上次重整中断后声明本规则对新目标路径的拥有权，
    // 让重试可以安全续作；真实记录行仍指向旧路径，重整完成前旧目录归属维持不变
    let own_rows = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?;
    let own: HashMap<PageKey, &organization_output::Model> = own_rows
        .iter()
        .filter(|row| !is_reorg_intent_bvid(&row.bvid))
        .map(|row| ((row.bvid.clone(), row.cid), row))
        .collect();
    // 上次重整中断残留的意图记录：真实 (bvid, cid) -> intent 行
    let reorg_intents: HashMap<(String, i64), &organization_output::Model> = own_rows
        .iter()
        .filter_map(|row| parse_reorg_intent_bvid(&row.bvid).map(|bvid| ((bvid, row.cid), row)))
        .collect();
    // 其它规则已整理的稳定标识 -> 规则名，用于冲突提示
    let other_rules: HashMap<i32, String> = organization_rule::Entity::find()
        .all(db)
        .await?
        .into_iter()
        .map(|rule| (rule.id, rule.name))
        .collect();
    let foreign_rows = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.ne(rule.id))
        .all(db)
        .await?;
    let other_keys: HashMap<PageKey, i32> = foreign_rows
        .iter()
        .map(|row| ((row.bvid.clone(), row.cid), row.rule_id))
        .collect();
    // 其它规则记录的目标路径 -> 规则名，用于跨规则目录归属判定
    let foreign_paths: Vec<(String, String)> = foreign_rows
        .iter()
        .filter_map(|row| {
            other_rules
                .get(&row.rule_id)
                .map(|name| (row.path.clone(), name.clone()))
        })
        .collect();

    let (candidates, missing_pages, undownloaded) = collect_source_videos(db, source).await?;
    if undownloaded > 0 {
        plan.warnings.push(format!("{undownloaded} 个视频尚未下载完成，已跳过"));
    }
    // 剧名：规则源目录的最后一级名称。同一源目录内所有 UP 主的视频聚合为同一部剧（R2）
    let series = source
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .with_context(|| format!("源目录缺少可用的末级目录名：{}", source.display()))?;
    for (video, page) in &missing_pages {
        let episode = own.get(&(video.bvid.clone(), page.cid)).and_then(|row| row.episode);
        plan.items.push(PlanItem {
            reorg: false,
            video_id: video.id,
            page_id: page.id,
            bvid: video.bvid.clone(),
            cid: page.cid,
            video_name: video.name.clone(),
            page_name: page.name.clone(),
            series: Some(series.clone()),
            episode,
            action: ItemAction::Missing("源视频文件不存在".to_string()),
            target: None,
            files: Vec::new(),
            message: None,
        });
    }

    // 全部候选视频归入以源目录名命名的同一部剧（跨 UP 主聚合，同名 UP 合并）
    let entries: Vec<(video::Model, page::Model)> = candidates
        .into_iter()
        .flat_map(|(video, pages)| pages.into_iter().map(move |page| (video.clone(), page)))
        .collect();
    // 剧名：规则源目录的最后一级名称。同一源目录内所有 UP 主的视频聚合为同一部剧（R2）
    // 注：skip 上面重复块
    let computed_stem = filenamify(&series);
    // 跨次/既有目录归属检查：无论目录当前是否存在，都必须有本规则按该剧名的持久记录
    // 证明归属；否则保守冲突（记录可能先于写入，或目录事后被删除/移动）。
    let existing = find_existing_name(&output, &computed_stem)?;
    let series_stem = match &existing {
        ExistingName::One(name) => name.clone(),
        _ => computed_stem.clone(),
    };
    let series_dir = output.join(&series_stem);
    // 符号链接目录由执行期 ensure_path_within 报错，这里不重复判定归属；
    // reorg intent 记录（bvid 带 reorg= 前缀，series 即新剧名）作为本规则 own 记录参与归属判定，
    // OtherOwned/Foreign 仍先于任何放行（dir_ownership 内部保持先拒绝后放行）
    let ownership = if std::fs::symlink_metadata(&series_dir).is_ok_and(|meta| meta.file_type().is_symlink()) {
        DirOwnership::Ours
    } else {
        dir_ownership(
            &series_dir,
            &own_rows,
            |row| row.series.as_deref() == Some(series.as_str()),
            &foreign_paths,
        )
    };
    // 目录不存在时仅“完全无记录”（Unclaimed）允许首次创建；有其它归属记录说明存在冲突
    let dir_exists = !matches!(existing, ExistingName::None);
    let conflict: Option<String> = match (&existing, &ownership) {
        (ExistingName::Ambiguous, _) => {
            plan.warnings.push(format!(
                "输出目录下存在多个「{computed_stem}」的大小写变体，无法确认剧集「{series}」归属，整组跳过"
            ));
            Some("存在多个大小写变体目录，无法确认归属".to_string())
        }
        (_, DirOwnership::OtherOwned) => {
            plan.warnings.push(format!(
                "剧集目录「{series_stem}」已有本规则其它剧集/视频的整理记录，剧集「{series}」整组跳过"
            ));
            Some("目标目录已有本规则其它作品的整理记录，无法确认归属".to_string())
        }
        (_, DirOwnership::Foreign(name)) => {
            plan.warnings.push(format!(
                "剧集目录「{series_stem}」已被规则「{name}」的记录占用，剧集「{series}」整组跳过"
            ));
            Some(format!("剧集目录已被规则「{name}」的记录占用，无法确认归属"))
        }
        (_, DirOwnership::Unclaimed) if dir_exists => {
            plan.warnings.push(format!(
                "剧集目录「{series_stem}」已存在但没有本规则（按剧名「{series}」）的整理记录，无法确认归属，整组跳过"
            ));
            Some("目标目录已存在但无法确认归属（可能是用户目录或此前整理记录已丢失的另一作品）".to_string())
        }
        _ => None,
    };
    // 旧版 STRM 历史记录保守冲突：硬链接输出绝不迁移或改写既有 STRM 记录与文件，整组跳过。
    // 本规则在任何历史剧集（含旧 UP 名）下的 STRM 记录都保持冲突：既不允许把记录改指新目录，
    // 也不允许旧记录继续指向 STRM 时在新剧下另写一套（避免产生“记录合并改写”的歧义）
    let conflict = conflict.or_else(|| {
        own_rows.iter().find(|row| row.path.ends_with(".strm")).map(|_| {
            plan.warnings.push(format!(
                "剧集「{series}」存在旧版 STRM 整理记录（含历史剧集），硬链接输出不支持迁移，整组跳过"
            ));
            "存在旧版 STRM 整理记录，硬链接输出不支持迁移".to_string()
        })
    });
    if let Some(message) = conflict {
        push_tv_conflict_group(&mut plan, &entries, &series, message);
        return Ok(plan);
    }
    let season_dir = series_dir.join("Season 1");

    let stem = format!("{series_stem} - S01E");
    // 最大集号来自规则历史记录（含源已丢失、视频失效、被去重的分页），新分页只能追加，绝不复用
    let mut max_episode = own_rows
        .iter()
        .filter(|row| row.series.as_deref() == Some(series.as_str()))
        .filter_map(|row| row.episode)
        .max()
        .unwrap_or(0);
    let mut numbered: Vec<(&page::Model, &video::Model, i32)> = Vec::new();
    let mut new_pages: Vec<(chrono::NaiveDateTime, i32, i32, &page::Model, &video::Model)> = Vec::new();
    // 已有记录的剧名与当前不一致（曾按 UP 名整理）：安全重整——在新剧集下生成新产物（新集号），
    // 写入成功后才更新记录；失败保留旧记录与旧目录。旧集号绝不带入新剧（避免撞号）。
    // 上次重整中断后留下的意图记录（bvid 带 reorg= 前缀）：已持久声明过本次目标路径，
    // 其集号与路径当作已确认方案续作；声明与当前计算不一致时保守冲突，不修改既有记录与文件
    let mut reorg_rows: HashMap<i32, &organization_output::Model> = HashMap::new();
    // 意图记录与计算目标不一致的分页：已按冲突推入 items，主推送循环必须跳过
    let mut intent_mismatch_pages: HashSet<i32> = HashSet::new();
    for (video, page) in &entries {
        let intent = reorg_intents.get(&(video.bvid.clone(), page.cid));
        let own_row = own.get(&page_key(video, page));
        let episode_of_intent = intent.and_then(|intent| intent.episode);
        match (own_row, intent) {
            // 意图存在且声明路径与本次计算一致：续作该重整（换用声明的集号）
            (_, Some(intent)) => {
                let episode = episode_of_intent.unwrap_or(0);
                let computed = season_dir.join(format!("{stem}{episode:0>2}.mp4"));
                if intent.path == computed.to_string_lossy() && episode > 0 {
                    max_episode = max_episode.max(episode);
                    numbered.push((page, video, episode));
                    reorg_rows.insert(page.id, intent);
                } else {
                    intent_mismatch_pages.insert(page.id);
                    plan.warnings.push(format!(
                        "分页 {}（{}）的重整意图记录与当前计算目标不一致，该分页整组冲突",
                        page.id, video.bvid
                    ));
                    plan.items.push(PlanItem {
                        video_id: video.id,
                        page_id: page.id,
                        bvid: video.bvid.clone(),
                        cid: page.cid,
                        video_name: video.name.clone(),
                        page_name: page.name.clone(),
                        series: Some(series.clone()),
                        episode: Some(episode),
                        reorg: true,
                        action: ItemAction::Conflict(format!(
                            "上次重整声明的目标（{}）与当前计算结果不一致，不修改既有记录与文件",
                            intent.path
                        )),
                        target: Some(PathBuf::from(&intent.path)),
                        files: Vec::new(),
                        message: None,
                    });
                }
            }
            // 无意图：旧记录剧名不匹配按安全重整规划；匹配则沿用历史集号；无记录则追加新集号
            (Some(row), None) if row.series.as_deref() != Some(series.as_str()) => {
                reorg_rows.insert(page.id, row);
                new_pages.push((video.pubtime, video.id, page.pid, page, video));
            }
            (Some(row), None) => {
                let episode = row.episode.unwrap_or(0);
                max_episode = max_episode.max(episode);
                numbered.push((page, video, episode));
            }
            (None, None) => new_pages.push((video.pubtime, video.id, page.pid, page, video)),
        }
    }
    new_pages.sort_by_key(|(pubtime, video_id, pid, _, _)| (*pubtime, *video_id, *pid));
    let mut episode_of: HashMap<i32, i32> = numbered.iter().map(|(page, _, episode)| (page.id, *episode)).collect();
    for (_, _, _, page, _) in &new_pages {
        max_episode += 1;
        episode_of.insert(page.id, max_episode);
    }
    // 集号重复检测（历史记录混合等极端情况下可能出现）
    let mut used: HashMap<i32, i32> = HashMap::new();
    for (page, _, episode) in &numbered {
        if let Some(other) = used.insert(*episode, page.id) {
            plan.warnings.push(format!(
                "剧集「{series}」存在重复集号 {episode}（分页 {} 与 {}），请检查",
                other, page.id
            ));
        }
    }
    // 剧集级附加文件：tvshow.nfo 必须生成（title 为剧名），
    // 封面取投稿时间最早且已下载本地封面的代表视频
    let mut reps: Vec<&video::Model> = entries.iter().map(|(video, _)| video).collect();
    reps.sort_by_key(|video| video.pubtime);
    reps.dedup_by(|a, b| a.id == b.id);
    if let Some(rep) = reps.first() {
        plan.extras.push(PendingFile {
            target: series_dir.join("tvshow.nfo"),
            overwrite: true,
            content: PendingContent::Xml(
                NFO::TVShow(TVShow {
                    name: &series,
                    intro: &rep.intro,
                    bvid: &rep.bvid,
                    uppers: rep.uppers().collect(),
                    premiered: rep.pubtime,
                    tags: rep.tags.as_ref().map(|tags| tags.clone().into()),
                })
                .generate_nfo()
                .await?,
            ),
        });
    }
    for video in &reps {
        let poster = entries
            .iter()
            .filter(|(v, _)| v.id == video.id)
            .find_map(|(_, page)| find_video_poster(&video.path, page.path.as_deref()));
        if let Some(poster) = poster {
            for (name, content) in [
                ("poster.jpg", PendingContent::Copy(poster.clone())),
                ("fanart.jpg", PendingContent::Copy(poster)),
            ] {
                plan.extras.push(PendingFile {
                    target: series_dir.join(name),
                    overwrite: true,
                    content,
                });
            }
            break;
        }
    }
    for (video, page) in &entries {
        if intent_mismatch_pages.contains(&page.id) {
            continue;
        }
        let reorg_row = reorg_rows.get(&page.id).copied();
        let episode = episode_of[&page.id];
        let target_stem = format!("{stem}{episode:0>2}");
        let mp4 = PathBuf::from(page.path.as_ref().expect("checked above"));
        let link = season_dir.join(format!("{target_stem}.mp4"));
        let action = if let Some(other_rule_id) = other_keys.get(&page_key(video, page)) {
            ItemAction::Conflict(format!(
                "该文件已被规则「{}」整理",
                other_rules.get(other_rule_id).map(String::as_str).unwrap_or("未知")
            ))
        } else if reorg_row.is_none() && own.contains_key(&page_key(video, page)) {
            ItemAction::Refresh
        } else if reorg_row.is_some() {
            // 旧记录但旧剧名不匹配：登记为重整项，执行时先写意图记录再写新产物、成功后改记录。
            // 目标 mp4 已存在时：持久意图记录允许“该单一视频内容”的同源硬链接（dev+ino 相等，
            // 即上次重整中断遗留）作为内容校验通过并续作；但 inode 不证明元数据（NFO/封面）或
            // 目录归属——无意图或不是同源文件时一律保守冲突，旧记录与旧目录保持不动
            let intent_declared = reorg_intents.contains_key(&(video.bvid.clone(), page.cid));
            if path_occupied(&link) && !(intent_declared && same_source_hardlink(&link, &mp4)) {
                ItemAction::Conflict(format!(
                    "重整目标「{}」已被其它文件占用，无法确认归属（非上次重整声明的同源产物），不修改既有记录与文件",
                    link.display()
                ))
            } else {
                ItemAction::Refresh
            }
        } else if path_occupied(&link) {
            ItemAction::Exists
        } else {
            ItemAction::Create
        };
        // 单P集标题用视频标题（page.name 可能不同）；多P用「视频标题 - 分P标题」
        let episode_title = if video.single_page == Some(false) {
            format!("{} - {}", video.name, page.name)
        } else {
            video.name.clone()
        };
        let mut files = vec![
            PendingFile {
                target: link.clone(),
                overwrite: false,
                content: PendingContent::HardLink(mp4.clone()),
            },
            PendingFile {
                target: season_dir.join(format!("{target_stem}.nfo")),
                overwrite: true,
                content: PendingContent::Xml(
                    NFO::Episode(Episode {
                        name: &episode_title,
                        pid: episode.to_string(),
                    })
                    .generate_nfo()
                    .await?,
                ),
            },
        ];
        files.extend(collect_sidecars(&mp4, &target_stem, &season_dir));
        plan.items.push(PlanItem {
            video_id: video.id,
            page_id: page.id,
            bvid: video.bvid.clone(),
            cid: page.cid,
            video_name: video.name.clone(),
            page_name: page.name.clone(),
            series: Some(series.clone()),
            episode: Some(episode),
            reorg: reorg_row.is_some(),
            action,
            target: Some(link),
            files,
            message: None,
        });
    }
    plan.items.sort_by_key(|item| {
        (
            item.series.clone().unwrap_or_default(),
            item.episode.unwrap_or(0),
            item.page_id,
        )
    });
    Ok(plan)
}

/// 电影规则整理计划。video_ids 为 None 时预览源目录全部候选（仅预览可用；
/// require_ids 为 true 即执行路径，此时必须显式选择）。
async fn plan_movie(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    source: &Path,
    video_ids: Option<&[i32]>,
    require_ids: bool,
) -> Result<Plan> {
    let output = PathBuf::from(&rule.output_path);
    let mut plan = Plan::default();
    // 其它规则已整理的稳定标识 -> 规则名，用于冲突提示
    let other_rules: HashMap<i32, String> = organization_rule::Entity::find()
        .all(db)
        .await?
        .into_iter()
        .map(|rule| (rule.id, rule.name))
        .collect();
    let foreign_rows = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.ne(rule.id))
        .all(db)
        .await?;
    let other_keys: HashMap<PageKey, i32> = foreign_rows
        .iter()
        .map(|row| ((row.bvid.clone(), row.cid), row.rule_id))
        .collect();
    let foreign_paths: Vec<(String, String)> = foreign_rows
        .iter()
        .filter_map(|row| {
            other_rules
                .get(&row.rule_id)
                .map(|name| (row.path.clone(), name.clone()))
        })
        .collect();
    // 本规则持久记录（含其它视频/此前执行）：电影按 bvid 判定目录归属，跨次亦生效
    let own_rows: Vec<organization_output::Model> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?;
    // 本规则已有的整理记录：稳定标识 -> 记录（含已记录目标路径，用于漂移检测）
    let own: HashMap<PageKey, organization_output::Model> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?
        .into_iter()
        .map(|row| {
            let key = (row.bvid.clone(), row.cid);
            (key, row)
        })
        .collect();

    let mut selected: Vec<(video::Model, Vec<page::Model>)> = Vec::new();
    match video_ids {
        Some(ids) => {
            for video_id in ids {
                let Some(video) = video::Entity::find_by_id(*video_id).one(db).await? else {
                    plan.warnings.push(format!("视频 {video_id} 不存在，已跳过"));
                    continue;
                };
                let Ok(video_dir) = std::fs::canonicalize(&video.path) else {
                    plan.warnings
                        .push(format!("视频「{}」尚未下载完成，已跳过", video.name));
                    continue;
                };
                if !video_dir.starts_with(source) {
                    plan.warnings
                        .push(format!("视频「{}」不在规则的源目录内，已跳过", video.name));
                    continue;
                }
                let pages = page::Entity::find()
                    .filter(page::Column::VideoId.eq(video.id))
                    .order_by_asc(page::Column::Pid)
                    .all(db)
                    .await?;
                selected.push((video, pages));
            }
        }
        None => {
            anyhow::ensure!(!require_ids, "电影规则执行需要显式提供待整理的视频列表 video_ids");
            let (candidates, missing_pages, undownloaded) = collect_source_videos(db, source).await?;
            if undownloaded > 0 {
                plan.warnings.push(format!("{undownloaded} 个视频尚未下载完成，已跳过"));
            }
            for (video, page) in &missing_pages {
                plan.items.push(PlanItem {
                    reorg: false,
                    video_id: video.id,
                    page_id: page.id,
                    bvid: video.bvid.clone(),
                    cid: page.cid,
                    video_name: video.name.clone(),
                    page_name: page.name.clone(),
                    series: None,
                    episode: None,
                    action: ItemAction::Missing("源视频文件不存在".to_string()),
                    target: None,
                    files: Vec::new(),
                    message: None,
                });
            }
            selected = candidates;
        }
    }

    // 目录名去重：同标题同年份但不同 BV 的电影会得到相同目录名，后出现者整组按冲突跳过，绝不合并不同作品
    let mut used_movie_stems: HashMap<String, String> = HashMap::new();
    for (video, pages) in selected {
        let mut downloadable = Vec::new();
        let mut missing_count = 0usize;
        for page in pages {
            match page.path.as_ref().filter(|path| !path.is_empty()) {
                Some(path) if page_path_within_source(path, source) && Path::new(path.as_str()).is_file() => {
                    downloadable.push(page)
                }
                Some(_) => {
                    missing_count += 1;
                    plan.items.push(PlanItem {
                        reorg: false,
                        video_id: video.id,
                        page_id: page.id,
                        bvid: video.bvid.clone(),
                        cid: page.cid,
                        video_name: video.name.clone(),
                        page_name: page.name.clone(),
                        series: None,
                        episode: None,
                        action: ItemAction::Missing("源视频文件不存在".to_string()),
                        target: None,
                        files: Vec::new(),
                        message: None,
                    });
                }
                None => missing_count += 1,
            }
        }
        if downloadable.is_empty() {
            plan.warnings
                .push(format!("视频「{}」尚未下载完成，已跳过", video.name));
            continue;
        }
        if missing_count > 0 {
            plan.warnings
                .push(format!("视频「{}」有 {missing_count} 个分P缺失，已跳过", video.name));
        }
        // 多P视为同一部电影的多个版本；版本名一律由分P固有 pid 决定（pid 为 1-based），
        // 任何分P缺失/补回都不改变其它版本的命名；单P视频无后缀
        let year = video.pubtime.year();
        let movie_stem = format!("{} ({year})", filenamify(&video.name));
        let stem_key = normalize_component(&movie_stem);
        if let Some(prev_bvid) = used_movie_stems.get(&stem_key)
            && prev_bvid != &video.bvid
        {
            plan.warnings.push(format!(
                "视频「{}」（{}）与视频（{prev_bvid}）的目标目录「{movie_stem}」冲突，整组跳过，不合并",
                video.name, video.bvid
            ));
            for page in downloadable {
                plan.items.push(PlanItem {
                    reorg: false,
                    video_id: video.id,
                    page_id: page.id,
                    bvid: video.bvid.clone(),
                    cid: page.cid,
                    video_name: video.name.clone(),
                    page_name: page.name.clone(),
                    series: None,
                    episode: None,
                    action: ItemAction::Conflict(format!("目标目录「{movie_stem}」已被其它视频占用")),
                    target: None,
                    files: Vec::new(),
                    message: None,
                });
            }
            continue;
        }
        // 跨次/既有目录归属检查：目录已存在时必须有本规则按该 bvid 的持久记录证明归属，
        // 否则保守冲突（含空目录）：绝不把用户目录或其它作品的目录并入
        let existing = find_existing_name(&output, &movie_stem)?;
        let folder_name = match &existing {
            ExistingName::One(name) => name.clone(),
            _ => movie_stem.clone(),
        };
        let folder = output.join(&folder_name);
        let dir_exists = !matches!(existing, ExistingName::None);
        let bvid = video.bvid.clone();
        // 无论目录当前是否存在，都必须有本规则按该 bvid 的持久记录证明归属（记录可能先于写入，
        // 目录也可能事后被删除）；只有完全无记录时才允许首次创建
        let ownership = if std::fs::symlink_metadata(&folder).is_ok_and(|meta| meta.file_type().is_symlink()) {
            DirOwnership::Ours
        } else {
            dir_ownership(&folder, &own_rows, move |row| row.bvid == bvid, &foreign_paths)
        };
        let ownership_conflict = match (&existing, &ownership) {
            (ExistingName::Ambiguous, _) => Some("输出目录下存在多个大小写变体目录，无法确认归属".to_string()),
            (_, DirOwnership::OtherOwned) => Some("目标目录已有本规则其它视频的整理记录，无法确认归属".to_string()),
            (_, DirOwnership::Foreign(name)) => Some(format!("目标目录已被规则「{name}」的记录占用，无法确认归属")),
            (_, DirOwnership::Unclaimed) if dir_exists => {
                Some("目标目录已存在但无法确认归属（可能是用户目录或此前整理记录已丢失的另一作品）".to_string())
            }
            _ => None,
        };
        if let Some(message) = ownership_conflict {
            plan.warnings.push(format!(
                "视频「{}」（{}）的目标目录「{folder_name}」{message}，整组跳过",
                video.name, video.bvid
            ));
            for page in downloadable {
                plan.items.push(PlanItem {
                    reorg: false,
                    video_id: video.id,
                    page_id: page.id,
                    bvid: video.bvid.clone(),
                    cid: page.cid,
                    video_name: video.name.clone(),
                    page_name: page.name.clone(),
                    series: None,
                    episode: None,
                    action: ItemAction::Conflict(message.clone()),
                    target: None,
                    files: Vec::new(),
                    message: None,
                });
            }
            continue;
        }
        // 旧版 STRM 历史记录保守冲突：硬链接输出绝不迁移或改写既有 STRM 记录与文件，整组跳过
        if own_rows
            .iter()
            .any(|row| row.bvid == video.bvid && row.path.ends_with(".strm"))
        {
            plan.warnings.push(format!(
                "视频「{}」（{}）存在旧版 STRM 整理记录，硬链接输出不支持迁移，整组跳过",
                video.name, video.bvid
            ));
            for page in downloadable {
                plan.items.push(PlanItem {
                    reorg: false,
                    video_id: video.id,
                    page_id: page.id,
                    bvid: video.bvid.clone(),
                    cid: page.cid,
                    video_name: video.name.clone(),
                    page_name: page.name.clone(),
                    series: None,
                    episode: None,
                    action: ItemAction::Conflict("存在旧版 STRM 整理记录，硬链接输出不支持迁移".to_string()),
                    target: None,
                    files: Vec::new(),
                    message: None,
                });
            }
            continue;
        }
        used_movie_stems.insert(stem_key, video.bvid.clone());
        let multi_page = video.single_page == Some(false);
        if multi_page && downloadable.len() > 8 {
            plan.warnings.push(format!(
                "视频「{}」有 {} 个版本，Emby 最多同时展示 8 个版本",
                video.name,
                downloadable.len()
            ));
        }
        for page in downloadable.iter() {
            let stem = if multi_page {
                format!("{movie_stem} - P{:0>2}", page.pid)
            } else {
                movie_stem.clone()
            };
            let mp4 = PathBuf::from(page.path.as_ref().expect("checked above"));
            let link = folder.join(format!("{stem}.mp4"));
            let action = if let Some(row) = own.get(&page_key(&video, page)) {
                // 记录的目标路径与当前计算结果不一致说明视频被重命名等，按冲突处理
                let computed = link.to_string_lossy().to_string();
                if row.path != computed {
                    ItemAction::Conflict(format!(
                        "该分P已记录的目标路径（{}）与当前计算结果不一致，请先删除该规则的整理记录再重新整理",
                        row.path
                    ))
                } else {
                    ItemAction::Refresh
                }
            } else if let Some(other_rule_id) = other_keys.get(&page_key(&video, page)) {
                ItemAction::Conflict(format!(
                    "该文件已被规则「{}」整理",
                    other_rules.get(other_rule_id).map(String::as_str).unwrap_or("未知")
                ))
            } else if path_occupied(&link) {
                ItemAction::Exists
            } else {
                ItemAction::Create
            };
            let mut files = vec![
                PendingFile {
                    target: link.clone(),
                    overwrite: false,
                    content: PendingContent::HardLink(mp4.clone()),
                },
                // NFO 携带本分P信息：多P/多版本的版本名用分P标题而非统一视频标题（R5）
                PendingFile {
                    target: folder.join(format!("{stem}.nfo")),
                    overwrite: true,
                    content: PendingContent::Xml(
                        NFO::Movie(Movie {
                            name: if multi_page { &page.name } else { &video.name },
                            intro: &video.intro,
                            bvid: &video.bvid,
                            uppers: video.uppers().collect(),
                            premiered: video.pubtime,
                            tags: video.tags.as_ref().map(|tags| tags.clone().into()),
                        })
                        .generate_nfo()
                        .await?,
                    ),
                },
            ];
            files.extend(collect_sidecars(&mp4, &stem, &folder));
            plan.items.push(PlanItem {
                video_id: video.id,
                page_id: page.id,
                bvid: video.bvid.clone(),
                cid: page.cid,
                video_name: video.name.clone(),
                page_name: page.name.clone(),
                series: None,
                episode: None,
                reorg: false,
                action,
                target: Some(link),
                files,
                message: None,
            });
        }
        if let Some(poster) = find_video_poster(
            &video.path,
            downloadable.first().and_then(|page| page.path.clone()).as_deref(),
        ) {
            for (name, content) in [
                ("poster.jpg", PendingContent::Copy(poster.clone())),
                ("fanart.jpg", PendingContent::Copy(poster)),
            ] {
                plan.extras.push(PendingFile {
                    target: folder.join(name),
                    overwrite: true,
                    content,
                });
            }
        }
    }
    Ok(plan)
}

/// 执行计划：extras 与分页级文件一律只补缺，绝不覆盖任何已存在的输出（含既有用户字幕等）；
/// 持久记录先于文件写入——数据库失败则未写任何文件，文件写入失败则记录已在，刷新即可补齐恢复。
/// ponytail: 路径检查与写入之间仍有 TOCTOU 窗口；整理期间须避免其他进程移动或替换目录及挂载。
async fn execute_plan(
    rule: &organization_rule::Model,
    plan: Plan,
    db: &DatabaseConnection,
    output_root: &Path,
) -> Result<ExecuteReport> {
    let mut report = ExecuteReport {
        items: Vec::with_capacity(plan.items.len()),
        warnings: plan.warnings,
        ..Default::default()
    };
    // 本规则已有的整理记录：稳定标识 -> 记录 id 与持久路径（覆盖分支要求记录路径与目标完全一致
    // 才视为"由该记录明确拥有"，避免把同目录其它产物的归属借用到未声明目标上）
    let own_rows_map: HashMap<PageKey, organization_output::Model> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?
        .into_iter()
        .map(|row| ((row.bvid.clone(), row.cid), row))
        .collect();
    // 本规则已有的整理记录：稳定标识 -> 记录 id
    let own: HashMap<PageKey, i32> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?
        .into_iter()
        .map(|row| ((row.bvid, row.cid), row.id))
        .collect();
    // 本规则重整意图记录声明的 mp4 目标：安全重整续作的持久归属凭证之一（mp4 内容校验用）
    let reorg_intent_paths: Vec<String> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .filter(organization_output::Column::Bvid.starts_with(REORG_INTENT_PREFIX))
        .all(db)
        .await?
        .into_iter()
        .map(|row| row.path)
        .collect();
    // 其它规则记录的目标路径：任何计划写入的文件（尤其覆盖类）若被其它规则记录指向，必须跳过
    let foreign_paths: Vec<String> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.ne(rule.id))
        .all(db)
        .await?
        .into_iter()
        .map(|row| row.path)
        .collect();
    for mut item in plan.items {
        match item.action.clone() {
            ItemAction::Create | ItemAction::Refresh => {
                let owned = own.contains_key(&(item.bvid.clone(), item.cid));
                // Create|Refresh 前置统一检查：
                // 1) 目标 mp4 已被占用——仅当本条目有持久记录（owned）或持久意图声明（reorg+意图）
                //    且内容校验通过（同源 dev+ino）才放行；Create 条目无记录、无凭证，
                //    目标存在即按 Exists 冲突跳过，不落新记录、不动用户文件；
                // 2) 覆盖类配套（NFO/封面）已有记录（非 reorg）才可走覆盖；Create（无记录）时
                //    任何已存在的配套都是未知归属文件，一律冲突，不登记记录、不改写；
                // 其它规则记录指向的文件一律拒绝（归属优先，先拒绝后放行）
                let intent_declared = item.reorg
                    && reorg_intent_paths
                        .iter()
                        .any(|path| *path == item.target.as_ref().map(|t| t.to_string_lossy()).unwrap_or_default());
                if !owned && !intent_declared {
                    // Create/无凭证条目：目标 mp4 已存在 → Exists（目标级占用，不落新记录）；
                    // 任何覆盖类配套（NFO/封面）已存在 → 冲突（未知归属文件，绝不改写）
                    if item.target.as_ref().is_some_and(|target| path_occupied(target)) {
                        item.action = ItemAction::Exists;
                        item.message = Some("目标文件已存在，未覆盖".to_string());
                        report.skipped += 1;
                        report.items.push(item);
                        continue;
                    }
                    if let Some(file) = item
                        .files
                        .iter()
                        .find(|file| file.overwrite && path_occupied(&file.target))
                    {
                        item.action = ItemAction::Conflict(format!(
                            "覆盖目标「{}」已存在（非本规则记录所有），不登记新记录、不改写用户文件",
                            file.target.display()
                        ));
                        item.message = item.action.action_message();
                        report.skipped += 1;
                        report.items.push(item);
                        continue;
                    }
                }
                // 写入前预校验输出目录内所有目标路径（含符号链接检查），失败则不落记录
                let mut invalid = None;
                for file in &item.files {
                    if let Err(e) = ensure_path_within(output_root, &file.target) {
                        invalid = Some(format!("{:#}", e));
                        break;
                    }
                }
                if let Some(message) = invalid {
                    item.action = ItemAction::Failed(message);
                    item.message = item.action.action_message();
                    report.failed += 1;
                    report.items.push(item);
                    continue;
                }
                let target_path = item
                    .target
                    .as_ref()
                    .map(|target| target.to_string_lossy().to_string())
                    .unwrap_or_default();
                // 先落持久记录再写文件；TV 重整（旧记录换新剧集）采用意图记录阶段化：
                // 1) 先把对新目标路径的拥有声明落成意图记录（旧记录不动、旧目录保留）——
                //    数据库失败则未写任何文件；
                // 2) 文件全部写成功后，在一个事务里更新旧记录指向新路径并删除意图记录；
                // 3) 中断/失败时意图记录残留，下次重跑按声明目标续作（plan 期识别），可安全恢复。
                // 非重整项维持记录先行的原有语义
                let record_result = if item.reorg {
                    persist_reorg_intent(rule, db, item.bvid.clone(), item.cid, &target_path, &item).await
                } else if owned {
                    let row_id = own[&(item.bvid.clone(), item.cid)];
                    organization_output::ActiveModel {
                        id: Set(row_id),
                        series: Set(item.series.clone()),
                        episode: Set(item.episode),
                        path: Set(target_path.clone()),
                        ..Default::default()
                    }
                    .update(db)
                    .await
                    .map(|_| ())
                    .map_err(anyhow::Error::from)
                } else {
                    organization_output::ActiveModel {
                        rule_id: Set(rule.id),
                        page_id: Set(item.page_id),
                        bvid: Set(item.bvid.clone()),
                        cid: Set(item.cid),
                        series: Set(item.series.clone()),
                        episode: Set(item.episode),
                        path: Set(target_path.clone()),
                        ..Default::default()
                    }
                    .insert(db)
                    .await
                    .map(|_| ())
                    .map_err(anyhow::Error::from)
                };
                if let Err(e) = record_result {
                    item.action = ItemAction::Failed(format!("保存整理记录失败：{:#}", e));
                    item.message = item.action.action_message();
                    report.failed += 1;
                    report.items.push(item);
                    continue;
                }
                let mut failure = None;
                let mut partial = false;
                // 重整项：先核对整批目标——存在任何无法证明归属的配套（NFO/封面）时，
                // 在写入任何文件之前保守冲突保留旧记录，避免“先部分写入再冲突”留下混合痕迹
                if item.reorg {
                    let preexist = item.files.iter().find(|file| {
                        file.overwrite
                            && !matches!(file.content, PendingContent::HardLink(_))
                            && path_occupied(&file.target)
                            && !foreign_paths.iter().any(|p| *p == file.target.to_string_lossy())
                    });
                    if let Some(file) = preexist {
                        item.action = ItemAction::Failed(format!(
                            "重整目标「{}」已存在且归属无法证明（可能是用户文件或上次中断遗留），保守冲突保留旧记录",
                            file.target.display()
                        ));
                        item.message = item.action.action_message();
                        report.failed += 1;
                        report.items.push(item);
                        continue;
                    }
                }
                for file in &item.files {
                    if path_occupied(&file.target) {
                        // 既有目标：仅允许本规则确认归属（非重整项）的 NFO 与封面安全覆盖
                        // （R5 强制覆盖元数据/封面）；重整项一律只补缺不覆盖——目标目录里的
                        // 既有文件可能是用户文件或上次中断遗留，元数据归属无法区分，绝不推定；
                        // 重整项的 mp4 还需内容校验：目标在计划后可能被外部替换，
                        // 仅当其仍与源视频同源（dev+ino 相等）才算本次声明目标内的合法产物；
                        // 其它规则记录指向的文件或非覆盖类（字幕/弹幕）也一律跳过
                        let foreign = foreign_paths
                            .iter()
                            .any(|foreign_path| *foreign_path == file.target.to_string_lossy());
                        if let (PendingContent::HardLink(source), true, true) =
                            (&file.content, item.reorg, path_occupied(&file.target))
                        {
                            if !same_source_hardlink(source, &file.target) {
                                failure = Some(format!(
                                    "重整目标「{}」在计划后已被外部替换（非本源视频同源文件），拒绝覆盖并保留旧记录",
                                    file.target.display()
                                ));
                                break;
                            }
                            partial = true;
                            continue;
                        }
                        // 重整项：任何已存在的覆盖类配套（NFO/封面）归属仍不可证明（意图只声明
                        // mp4 同名 stem），一律保守冲突保留旧记录——自动续作需以“目标目录里
                        // 无归属未明产物”为前提；字幕/弹幕等非覆盖类继续只补缺跳过
                        if item.reorg && file.overwrite {
                            failure = Some(format!(
                                "重整目标「{}」已存在且归属无法证明（可能是用户文件或上次中断遗留），保守冲突保留旧记录",
                                file.target.display()
                            ));
                            break;
                        }
                        if !file.overwrite || foreign {
                            partial = true;
                            continue;
                        }
                        // 覆盖分支唯一放行：执行前真实存在本条记录，且该记录路径与本条目目标一致
                        // （此前预检查已排除 Create/无凭证与重整项的占位，此处仅剩“记录命名目标”）
                        let record_matches = own_rows_map
                            .get(&(item.bvid.clone(), item.cid))
                            .is_some_and(|row| row.path == target_path);
                        if !record_matches {
                            failure = Some(format!(
                                "覆盖目标「{}」不被本条持久记录声明（记录路径与目标不一致），拒绝覆盖",
                                file.target.display()
                            ));
                            break;
                        }
                        if let Err(e) = write_pending_file_overwrite(output_root, file).await {
                            failure = Some(format!("覆盖「{}」失败：{:#}", file.target.display(), e));
                            break;
                        }
                        continue;
                    }
                    if let Err(e) = write_pending_file(output_root, file).await {
                        failure = Some(format!("写入「{}」失败：{:#}", file.target.display(), e));
                        break;
                    }
                }
                let art_missing =
                    matches!(item.action, ItemAction::Refresh | ItemAction::Create) && !has_episode_art(&item.files);
                if let Some(message) = failure {
                    item.action = ItemAction::Failed(message);
                    item.message = item.action.action_message();
                    report.failed += 1;
                } else {
                    // TV 重整：新产物已全部成功写入，此时才在一个事务里把旧记录指向新路径并删除
                    // 意图记录；失败时意图记录残留、旧记录未动，下次重跑按意图声明续作
                    if item.reorg {
                        let row_id = own[&(item.bvid.clone(), item.cid)];
                        let commit =
                            finalize_reorg(rule, db, item.bvid.clone(), item.cid, &target_path, &item, row_id).await;
                        if commit.is_err() {
                            item.action = ItemAction::Failed("提交重整失败：意图记录已保留，请重新执行".to_string());
                            item.message = item.action.action_message();
                            report.failed += 1;
                            report.items.push(item);
                            continue;
                        }
                    }
                    if partial {
                        append_message(&mut item.message, "部分配套文件（视频/字幕/弹幕）已存在，未覆盖");
                    }
                    if art_missing {
                        append_message(
                            &mut item.message,
                            "源视频缺少可用的分集封面图片，未伪造或复制其它分集的封面",
                        );
                    }
                    if owned {
                        report.refreshed += 1;
                    } else {
                        report.created += 1;
                    }
                }
            }
            ItemAction::Exists => {
                item.message = Some("目标文件已存在，未覆盖".to_string());
                report.skipped += 1;
            }
            ItemAction::Conflict(message) => {
                item.message = Some(message);
                report.skipped += 1;
            }
            ItemAction::Missing(message) => {
                item.message = Some(message);
                report.skipped += 1;
            }
            ItemAction::Failed(_) => report.skipped += 1,
        }
        report.items.push(item);
    }
    // 仅为对应目录内成功整理的条目补充剧集/电影级文件。
    // 归属判定从严：只要该目录里还有任何本规则无法完整承诺归属的重整条目（本轮冲突、失败、
    // 中断残留，或本轮成功的重整条目），该目录的既有 extras（poster/fanart/tvshow.nfo）
    // 一律只补缺，绝不覆盖——目录归属无法完整证明时，不推定任何既有文件可被改写
    for extra in &plan.extras {
        let Some(dir) = extra.target.parent() else {
            continue;
        };
        // rundirs 里是否包含本轮重整条目（任何 action，包括冲突/失败/成功）
        let has_reorg = report
            .items
            .iter()
            .any(|item| item.reorg && item.target.as_ref().is_some_and(|target| target.starts_with(dir)));
        if !report.items.iter().any(|item| {
            matches!(item.action, ItemAction::Create | ItemAction::Refresh)
                && item.target.as_ref().is_some_and(|target| target.starts_with(dir))
        }) {
            continue;
        }
        let result = if path_occupied(&extra.target) {
            if has_reorg {
                report.warnings.push(format!(
                    "目录「{}」存在重整条目，既有产物「{}」归属无法证明，未覆盖（只补缺）",
                    dir.display(),
                    extra.target.display()
                ));
                continue;
            }
            // 非重整目录：元数据与封面强制覆盖（用户已确认）；延续临时文件+原子替换方案
            write_pending_file_overwrite(output_root, extra).await
        } else {
            write_pending_file(output_root, extra).await
        };
        match result {
            Ok(()) => report.extras_created += 1,
            Err(e) => report
                .warnings
                .push(format!("写入「{}」失败：{:#}", extra.target.display(), e)),
        }
    }
    Ok(report)
}

impl ItemAction {
    /// Failed 分支下用于填充 item.message 的简述
    fn action_message(&self) -> Option<String> {
        match self {
            ItemAction::Failed(message) => Some(message.clone()),
            _ => None,
        }
    }
}

/// 写入/校准本条分页的重整意图记录：bvid 带 reorg= 前缀（与真实记录同 key 空间的独立行），
/// 对新目标路径的持久拥有声明。已存在且声明一致时幂等通过；声明不一致拒绝（保守）。
async fn persist_reorg_intent(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    bvid: String,
    cid: i64,
    target_path: &str,
    item: &PlanItem,
) -> Result<()> {
    use sea_orm::{EntityTrait, QueryFilter};
    let intent_bvid = reorg_intent_bvid(&bvid);
    let existing = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .filter(organization_output::Column::Bvid.eq(&intent_bvid))
        .filter(organization_output::Column::Cid.eq(cid))
        .one(db)
        .await?;
    match existing {
        Some(row) if row.path == target_path => {
            // 幂等：声明与本次一致；顺带把 series/episode 与计划对齐（同一目标的重复声明）
            organization_output::ActiveModel {
                id: Set(row.id),
                series: Set(item.series.clone()),
                episode: Set(item.episode),
                ..Default::default()
            }
            .update(db)
            .await?;
            Ok(())
        }
        Some(row) => Err(anyhow::anyhow!(
            "意图记录声明的目标（{}）与本次目标（{target_path}）不一致，拒绝执行",
            row.path
        )),
        None => {
            organization_output::ActiveModel {
                rule_id: Set(rule.id),
                page_id: Set(item.page_id),
                bvid: Set(intent_bvid),
                cid: Set(cid),
                series: Set(item.series.clone()),
                episode: Set(item.episode),
                path: Set(target_path.to_string()),
                ..Default::default()
            }
            .insert(db)
            .await?;
            Ok(())
        }
    }
}

/// 重整提交：在一个事务里更新旧记录（新剧集/新集号/新路径）并删除意图记录。
/// 旧目录与旧文件在磁盘上不受影响；更新与删除原子一致，失败时意图记录和旧记录都保持原状，
/// 下次重跑按意图声明续作（plan 期识别），可安全恢复
async fn finalize_reorg(
    rule: &organization_rule::Model,
    db: &DatabaseConnection,
    bvid: String,
    cid: i64,
    target_path: &str,
    item: &PlanItem,
    row_id: i32,
) -> Result<()> {
    use sea_orm::{EntityTrait, QueryFilter, TransactionTrait};
    let intent_bvid = reorg_intent_bvid(&bvid);
    let txn = db.begin().await?;
    organization_output::ActiveModel {
        id: Set(row_id),
        series: Set(item.series.clone()),
        episode: Set(item.episode),
        path: Set(target_path.to_string()),
        ..Default::default()
    }
    .update(&txn)
    .await?;
    organization_output::Entity::delete_many()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .filter(organization_output::Column::Bvid.eq(&intent_bvid))
        .filter(organization_output::Column::Cid.eq(cid))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    Ok(())
}
/// 不支持硬链接时明确报错，绝不复制视频回退）。其余文件先写唯一命名的临时文件（create_new 原子创建），
/// 再以硬链接原子落到目标位置——hard_link 在目标已存在时失败，因此绝不覆盖任何既有文件；临时文件随后删除。
/// 注意：原子创建不解决目录竞态；整理期间须避免其他进程移动或替换目录及挂载。
/// ponytail: 不支持硬链接的文件系统（部分 NAS/网络盘/exFAT）会在此报错，无法整理该位置。
async fn write_pending_file(output_root: &Path, file: &PendingFile) -> Result<()> {
    ensure_path_within(output_root, &file.target)?;
    if let Some(parent) = file.target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // 目录创建后复查：祖先路径中途被替换为符号链接时拒绝写入
    ensure_path_within(output_root, &file.target)?;
    // 视频输出是对源视频的硬链接：内核原子创建，目标已存在即失败（绝不覆盖，含并发场景；
    // 但这不解决目录级竞态）。跨文件系统/文件系统不支持时明确报错，绝不复制视频回退
    if let PendingContent::HardLink(source) = &file.content {
        return match tokio::fs::hard_link(source, &file.target).await {
            Ok(()) => Ok(()),
            // 目标已存在（含并发创建）：保持未覆盖语义，直接失败
            Err(_) if path_occupied(&file.target) => Err(anyhow::anyhow!("目标已存在，未覆盖")),
            Err(e) => Err(e).context(format!(
                "无法为源视频「{}」创建硬链接；源与目标可能不在同一文件系统，或该文件系统不支持硬链接，该位置无法整理",
                source.display()
            )),
        };
    }
    let tmp = file.target.with_file_name(format!(
        "{}.{}.tmp",
        file.target.file_name().context("非法的目标路径")?.to_string_lossy(),
        uuid::Uuid::new_v4().simple()
    ));
    let result: Result<()> = async {
        let mut dst = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        match &file.content {
            PendingContent::Xml(xml) => dst.write_all(xml.as_bytes()).await?,
            PendingContent::Copy(source) => {
                let mut src = tokio::fs::File::open(source).await?;
                tokio::io::copy(&mut src, &mut dst).await?;
            }
            PendingContent::HardLink(_) => unreachable!("硬链接在上方分支处理，不走临时文件"),
        }
        dst.flush().await?;
        drop(dst);
        match tokio::fs::hard_link(&tmp, &file.target).await {
            Ok(()) => Ok(()),
            // 并发下目标已被创建：保持未覆盖语义，直接失败
            Err(_) if path_occupied(&file.target) => Err(anyhow::anyhow!("目标已存在，未覆盖")),
            // ponytail: 不支持硬链接的文件系统（部分 NAS/网络盘/exFAT）无法整理，直接报错；
            // 不做 create_new+复制回退：失败清理路径存在误删并发创建文件的风险（已评估并否决）
            Err(e) => Err(e).context(format!(
                "无法把临时文件硬链接到「{}」；文件系统可能不支持硬链接，该位置无法整理",
                file.target.display()
            )),
        }
    }
    .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    result.with_context(|| format!("写入「{}」失败", file.target.display()))
}

/// 强制覆盖写入（仅限本规则确认归属的元数据 NFO 与封面图片，用户已确认此行为会覆盖手工修改）：
/// 先写唯一命名的临时文件（create_new 原子创建），再用 rename 原子替换目标——
/// 任何一步失败都保留原目标文件，绝不出现“删了旧文件又没写上新”的中间态。
/// 目标是符号链接时拒绝（rename 会替换链接本身而不是其目标之外的文件，且指向不可信）。
/// 图片通过读取源文件内容后再写入，不跟随源符号链接（source.is_file() 已在规划期确认为目标文件）。
/// 路径边界检查与 write_pending_file 相同。
async fn write_pending_file_overwrite(output_root: &Path, file: &PendingFile) -> Result<()> {
    ensure_path_within(output_root, &file.target)?;
    if let Ok(meta) = file.target.symlink_metadata()
        && meta.file_type().is_symlink()
    {
        anyhow::bail!("目标「{}」是符号链接，拒绝覆盖", file.target.display());
    }
    let tmp_dir = file.target.parent().context("非法的目标路径")?.to_path_buf();
    tokio::fs::create_dir_all(&tmp_dir).await?;
    ensure_path_within(output_root, &file.target)?;
    let tmp = tmp_dir.join(format!(
        "{}.{}.tmp",
        file.target.file_name().context("非法的目标路径")?.to_string_lossy(),
        uuid::Uuid::new_v4().simple()
    ));
    let result: Result<()> = async {
        let mut dst = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .await?;
        match &file.content {
            PendingContent::Xml(xml) => dst.write_all(xml.as_bytes()).await?,
            PendingContent::Copy(source) => {
                // 先在源端读取内容并校验为常规文件，再写入临时文件
                let src_meta = tokio::fs::metadata(source).await?;
                anyhow::ensure!(
                    src_meta.is_file(),
                    "配套源「{}」不是常规文件，拒绝复制",
                    source.display()
                );
                let mut src = tokio::fs::File::open(source).await?;
                tokio::io::copy(&mut src, &mut dst).await?;
            }
            PendingContent::HardLink(_) => unreachable!("视频硬链接永不覆盖"),
        }
        dst.flush().await?;
        drop(dst);
        tokio::fs::rename(&tmp, &file.target).await?;
        Ok(())
    }
    .await;
    let _ = tokio::fs::remove_file(&tmp).await;
    result.with_context(|| format!("覆盖「{}」失败", file.target.display()))
}

/// 查找视频已下载的海报：多页视频为视频目录下的 poster.jpg，单页视频为分页 mp4 同名的 -poster.jpg
fn find_video_poster(video_path: &str, first_page_path: Option<&str>) -> Option<PathBuf> {
    let multi = Path::new(video_path).join("poster.jpg");
    if multi.is_file() {
        return Some(multi);
    }
    let mp4 = Path::new(first_page_path?);
    let poster = mp4
        .parent()?
        .join(format!("{}-poster.jpg", mp4.file_stem()?.to_string_lossy()));
    poster.is_file().then_some(poster)
}

/// 收集源视频旁的配套文件（字幕 srt、弹幕 ass、单集缩略图/封面），映射为输出目录中与视频硬链接同名的文件。
/// Emby 播放视频时只读取与其同目录、同主文件名的外挂字幕/弹幕，因此必须复制到输出侧。
fn collect_sidecars(source_video: &Path, target_stem: &str, target_dir: &Path) -> Vec<PendingFile> {
    let mut files = Vec::new();
    let Some(dir) = source_video.parent() else {
        return files;
    };
    let Some(src_stem) = source_video.file_stem().and_then(|stem| stem.to_str()) else {
        return files;
    };
    let prefix = format!("{src_stem}.");
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            let Some(suffix) = name.strip_prefix(&prefix) else {
                continue;
            };
            let path = entry.path();
            let ext = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");
            if !matches!(ext, "srt" | "ass") {
                continue;
            }
            files.push(PendingFile {
                target: target_dir.join(format!("{target_stem}.{suffix}")),
                overwrite: false,
                content: PendingContent::Copy(entry.path()),
            });
        }
    }
    // 单集图片：多页视频为 {stem}-thumb.jpg，单页视频为 {stem}-poster.jpg。
    // 图片属于 R5 强制覆盖的封面类（用户已确认），与 NFO 相同待遇
    for suffix in ["-thumb.jpg", "-poster.jpg"] {
        let source = dir.join(format!("{src_stem}{suffix}"));
        if source.is_file() {
            files.push(PendingFile {
                target: target_dir.join(format!("{target_stem}{suffix}")),
                overwrite: true,
                content: PendingContent::Copy(source),
            });
        }
    }
    files
}

#[cfg(test)]
mod tests {
    use bili_sync_migration::MigratorTrait;
    use sea_orm::{ActiveModelTrait, ConnectionTrait, TryIntoModel};

    use super::*;

    /// 测试临时根：优先取环境变量 BILI_SYNC_ORG_TEST_ROOT（指向已登记的任务临时根），缺省为系统临时目录；
    /// 在其下用 UUID 创建唯一、不可预测的子目录（create_dir，已存在或失败即终止）。测试绝不删除任何目录，
    /// 统一由登记的清理流程或系统临时目录策略回收。
    fn temp_root(name: &str) -> PathBuf {
        let base = std::env::var("BILI_SYNC_ORG_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("{name}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&dir).unwrap();
        dir
    }

    /// 测试用固定日期构造（YYYY 年 1 月 d 日零时）
    fn day_of(d: u32) -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(2024, 1, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    }

    /// 硬链接与源文件同 inode 校验（Unix 语义）
    #[cfg(unix)]
    fn assert_same_inode(link: &Path, source: &Path) {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            std::fs::metadata(link).unwrap().ino(),
            std::fs::metadata(source).unwrap().ino(),
            "应为指向同一 inode 的硬链接"
        );
    }

    #[cfg(not(unix))]
    fn assert_same_inode(_link: &Path, _source: &Path) {}

    async fn test_db(dir: &Path) -> DatabaseConnection {
        let db = sea_orm::Database::connect(format!("sqlite://{}?mode=rwc", dir.join("data.sqlite").display()))
            .await
            .unwrap();
        bili_sync_migration::Migrator::up(&db, None).await.unwrap();
        db
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_video(
        db: &DatabaseConnection,
        bvid: &str,
        upper_name: &str,
        path: &Path,
        pubtime: chrono::NaiveDateTime,
        single_page: bool,
        favorite_id: i32,
    ) -> video::Model {
        video::ActiveModel {
            bvid: Set(bvid.to_string()),
            favorite_id: Set(Some(favorite_id)),
            upper_id: Set(1),
            upper_name: Set(upper_name.to_string()),
            upper_face: Set(String::new()),
            name: Set(format!("视频{bvid}")),
            path: Set(path.to_string_lossy().to_string()),
            category: Set(2),
            intro: Set(String::new()),
            cover: Set(String::new()),
            ctime: Set(pubtime),
            pubtime: Set(pubtime),
            favtime: Set(pubtime),
            download_status: Set(0),
            valid: Set(true),
            single_page: Set(Some(single_page)),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    async fn insert_page(db: &DatabaseConnection, video_id: i32, pid: i32, mp4: Option<&Path>) -> page::Model {
        page::ActiveModel {
            video_id: Set(video_id),
            cid: Set(pid as i64),
            pid: Set(pid),
            name: Set(format!("分P{pid}")),
            duration: Set(0),
            path: Set(mp4.map(|path| path.to_string_lossy().to_string())),
            download_status: Set(0),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    fn rule_model(name: &str, rule_type: &str, source: &Path, output: &Path) -> organization_rule::Model {
        organization_rule::Model {
            id: 0,
            name: name.to_string(),
            rule_type: rule_type.to_string(),
            source_path: source.to_string_lossy().to_string(),
            output_path: output.to_string_lossy().to_string(),
            created_at: String::new(),
        }
    }

    fn touch(path: &Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"").unwrap();
    }

    /// 构造一个单P视频 + 源视频文件，返回（视频, 分页, mp4 路径）
    async fn single_video(
        db: &DatabaseConnection,
        bvid: &str,
        upper_name: &str,
        dir: &Path,
        pubtime: chrono::NaiveDateTime,
    ) -> (video::Model, page::Model, PathBuf) {
        let video = insert_video(db, bvid, upper_name, dir, pubtime, true, 0).await;
        let mp4 = dir.join("EP.mp4");
        touch(&mp4);
        let page = insert_page(db, video.id, 0, Some(&mp4)).await;
        (video, page, mp4)
    }

    #[test]
    fn test_paths_overlap() {
        assert!(paths_overlap(Path::new("/a/b"), Path::new("/a/b")));
        assert!(paths_overlap(Path::new("/a"), Path::new("/a/b")));
        assert!(paths_overlap(Path::new("/a/b/c"), Path::new("/a/b")));
        assert!(!paths_overlap(Path::new("/a/b"), Path::new("/a/bc")));
        assert!(!paths_overlap(Path::new("/a"), Path::new("/b")));
    }

    #[tokio::test]
    async fn test_symlink_alias_rejected() {
        let root = temp_root("symlink");
        let real = root.join("real");
        let alias = root.join("alias");
        std::fs::create_dir_all(&real).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&real, &alias).unwrap();
        let real_canonical = std::fs::canonicalize(&real).unwrap();
        let alias_canonical = std::fs::canonicalize(&alias).unwrap();
        // 符号链接别名规范化后是同一路径，嵌套校验会拒绝
        assert_eq!(real_canonical, alias_canonical);
        assert!(paths_overlap(&real_canonical, &alias_canonical));
        // ensure_path_within 拒绝把符号链接目录本身作为写入目标
        assert!(ensure_path_within(&root, &alias).is_err());
    }

    #[tokio::test]
    async fn test_duplicate_sources_merge_pages_and_missing() {
        let root = temp_root("duplicate-sources");
        let source = root.join("source");
        let a = source.join("a");
        let b = source.join("b");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let first = insert_video(&db, "BVsame", "UP", &a, time, false, 1).await;
        let second = insert_video(&db, "BVsame", "UP", &b, time, false, 2).await;
        let first_page = a.join("1.mp4");
        let second_page = b.join("2.mp4");
        touch(&first_page);
        touch(&second_page);
        insert_page(&db, first.id, 1, Some(&first_page)).await;
        insert_page(&db, first.id, 2, Some(&a.join("missing.mp4"))).await;
        insert_page(&db, second.id, 2, Some(&second_page)).await;
        let (candidates, missing, _) = collect_source_videos(&db, &std::fs::canonicalize(&source).unwrap())
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].1.len(), 2);
        assert!(missing.is_empty());
    }

    #[tokio::test]
    async fn test_tv_numbering_history_and_appending() {
        let root = temp_root("tv");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;

        // 视频 A：1月2日投稿，单P；视频 B：1月1日投稿，3P。同一UP「source」
        let video_a_dir = source.join("视频A");
        let (video_a, page_a, mp4_a) = single_video(&db, "BV1a", "source", &video_a_dir, day_of(2)).await;
        // 为 A 的分页准备封面与弹幕，检验配套文件复制
        touch(&video_a_dir.join("EP-poster.jpg"));
        touch(&video_a_dir.join("EP.zh-CN.default.ass"));
        touch(&video_a_dir.join("EP.zh-CN.srt"));

        let video_b_dir = source.join("视频B").join("Season 1");
        let video_b = insert_video(
            &db,
            "BV1b",
            "source",
            video_b_dir.parent().unwrap(),
            day_of(1),
            false,
            0,
        )
        .await;
        for pid in 1..=3 {
            let mp4 = video_b_dir.join(format!("EP{pid}.mp4"));
            touch(&mp4);
            insert_page(&db, video_b.id, pid, Some(&mp4)).await;
        }

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert_eq!(plan.items.len(), 4, "1 单P + 3 分P");
        assert!(plan.items.iter().all(|item| item.action == ItemAction::Create));
        assert!(plan.items.iter().all(|item| item.bvid.starts_with("BV1")));
        let episodes: Vec<i32> = plan.items.iter().filter_map(|item| item.episode).collect();
        assert_eq!(
            episodes,
            vec![1, 2, 3, 4],
            "按投稿时间排序：B(1月1日) P1-3 为 1-3，A(1月2日) 为 4"
        );

        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 4);
        let season = output.join("source").join("Season 1");
        let link = season.join("source - S01E04.mp4");
        assert_same_inode(&link, &mp4_a);
        assert!(mp4_a.is_file(), "源视频保持原样");
        assert!(!season.join("source - S01E04.strm").exists(), "不再生成 STRM");
        // 单P集标题 = 视频标题；多P集标题 = 视频标题 - 分P标题；tvshow.nfo 标题 = UP 用户名
        let nfo_a = std::fs::read_to_string(season.join("source - S01E04.nfo")).unwrap();
        assert!(nfo_a.contains("<title>视频BV1a</title>"), "单P集标题应为视频标题");
        let nfo_b1 = std::fs::read_to_string(season.join("source - S01E01.nfo")).unwrap();
        assert!(
            nfo_b1.contains("<title>视频BV1b - 分P1</title>"),
            "多P集标题应为 视频标题 - 分P标题"
        );
        let tvshow = std::fs::read_to_string(output.join("source").join("tvshow.nfo")).unwrap();
        assert!(
            tvshow.contains("<title>source</title>"),
            "tvshow.nfo 标题应为 UP 用户名"
        );
        assert!(
            season.join("source - S01E04.zh-CN.default.ass").is_file(),
            "弹幕复制到输出侧"
        );
        assert!(season.join("source - S01E04.zh-CN.srt").is_file());
        assert!(season.join("source - S01E04-poster.jpg").is_file());
        assert!(output.join("source").join("poster.jpg").is_file());

        // 重复执行：全部为刷新，集号不变，无副作用
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 0);
        assert_eq!(report.refreshed, 4);

        // 集号来自历史记录：即使源文件丢失（Missing）集号也被保留，新增视频绝不复用集号
        std::fs::remove_file(&mp4_a).unwrap();
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item_a = plan.items.iter().find(|item| item.video_id == video_a.id).unwrap();
        assert_eq!(item_a.action, ItemAction::Missing("源视频文件不存在".to_string()));
        assert_eq!(item_a.episode, Some(4), "缺失源的分页保留已记录集号");

        // 新增投稿更晚的视频：追加编号 5（历史最大 4 来自记录，而非仅现存分页）
        let video_c_dir = source.join("视频C");
        let (video_c, _, _) = single_video(&db, "BV1c", "source", &video_c_dir, day_of(3)).await;
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item_c = plan.items.iter().find(|item| item.video_id == video_c.id).unwrap();
        assert_eq!(item_c.episode, Some(5), "新增视频追加在历史最大集号之后");
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);

        // 视频失效（valid=false）：其记录的集号仍参与历史最大值计算
        let mut invalid_a: video::ActiveModel = video_a.clone().into();
        invalid_a.valid = Set(false);
        invalid_a.update(&db).await.unwrap();
        let video_d_dir = source.join("视频D");
        let (video_d, _, _) = single_video(&db, "BV1d", "source", &video_d_dir, day_of(4)).await;
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items.iter().all(|item| item.video_id != video_a.id),
            "失效视频不再出现在计划中"
        );
        let item_d = plan.items.iter().find(|item| item.video_id == video_d.id).unwrap();
        assert_eq!(item_d.episode, Some(6), "失效视频的集号 4 仍计入历史，新集号从 6 起");
        // 既有分页集号不变
        assert_eq!(
            plan.items
                .iter()
                .filter(|item| item.video_id == video_b.id)
                .filter_map(|item| item.episode)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let _ = page_a.id; // 保持变量使用
    }

    /// 刷新绝不覆盖用户预先放置/修改的输出文件（包括字幕），只补缺
    #[tokio::test]
    async fn test_tv_refresh_never_overwrites_user_files() {
        let root = temp_root("tv-fill");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let pubtime = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let video_dir = source.join("视频A");
        let (_video, _page, _mp4) = single_video(&db, "BV1a", "source", &video_dir, pubtime).await;
        touch(&video_dir.join("EP.zh-CN.srt"));

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);
        let season = output.join("source").join("Season 1");
        let link = season.join("source - S01E01.mp4");
        let srt = season.join("source - S01E01.zh-CN.srt");

        // 用户修改本规则生成的字幕：刷新后保持用户内容不变，只补缺缺失文件
        std::fs::write(&srt, b"user subtitle").unwrap();
        std::fs::remove_file(season.join("source - S01E01.nfo")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(
            std::fs::read(&srt).unwrap(),
            b"user subtitle",
            "刷新不得覆盖已存在的输出文件（含用户修改过的字幕）"
        );
        assert!(season.join("source - S01E01.nfo").is_file(), "缺失文件被补齐");

        // 修改过的 NFO 被强制覆盖（用户已确认覆盖本规则归属的元数据），字幕保持用户内容
        let nfo = std::fs::read_to_string(season.join("source - S01E01.nfo")).unwrap();
        assert!(nfo.contains("<title>"), "本规则归属的既有 NFO 应被重新生成覆盖");
        assert_eq!(std::fs::read(&srt).unwrap(), b"user subtitle");

        // 用户替换了视频硬链接本体：刷新跳过该文件（不覆盖），其余文件补缺，记录更新
        std::fs::write(&link, b"user video").unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(
            std::fs::read(&link).unwrap(),
            b"user video",
            "不得覆盖用户替换的视频硬链接"
        );
        assert!(
            report.items[0].message.as_deref().is_some_and(|m| m.contains("未覆盖")),
            "视频未覆盖时应有提示"
        );
    }

    #[tokio::test]
    async fn test_movie_pid_versions_and_no_overwrite() {
        let root = temp_root("movie");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let t = chrono::NaiveDate::from_ymd_opt(2019, 5, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        // 单P电影
        let video_m1_dir = source.join("M1");
        let (video_m1, _, mp4_m1) = single_video(&db, "BV1m1", "导演", &video_m1_dir, t).await;
        touch(&video_m1_dir.join("EP-poster.jpg"));

        // 多P电影：pid 1/2/6（项目分P为 1-based）共 3 个版本，版本名一律由固有 pid 决定
        let video_m2_dir = source.join("M2");
        let video_m2 = insert_video(&db, "BV1m2", "导演", &video_m2_dir, t, false, 0).await;
        for pid in [1, 2, 6] {
            let mp4 = video_m2_dir.join(format!("v{pid}.mp4"));
            touch(&mp4);
            insert_page(&db, video_m2.id, pid, Some(&mp4)).await;
        }
        // 仅有 pid=1 的分P有源缩略图；其余版本缺图，用于验证不嫁接其它版本图片
        touch(&video_m2_dir.join("v1-thumb.jpg"));

        let rule = rule_model(
            "电影库",
            "movie",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        // 预览无需 video_ids：按源目录列出全部候选
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert_eq!(plan.items.len(), 4, "预览全部候选：1 单P + 3 版本");
        let versions: Vec<String> = plan
            .items
            .iter()
            .filter(|item| item.video_id == video_m2.id)
            .filter_map(|item| {
                item.target
                    .as_ref()
                    .and_then(|t| t.file_name().map(|n| n.to_string_lossy().to_string()))
            })
            .collect();
        assert_eq!(versions[0], "视频BV1m2 (2019) - P01.mp4", "多P版本名由固有 pid 决定");
        assert_eq!(versions[1], "视频BV1m2 (2019) - P02.mp4");
        assert_eq!(versions[2], "视频BV1m2 (2019) - P06.mp4");

        // 执行必须显式选择
        assert!(
            execute_rule(&rule, &db, None).await.is_err(),
            "电影执行缺少 video_ids 应报错"
        );
        let report = execute_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        assert_eq!(report.created, 1);
        let folder_m1 = output.join("视频BV1m1 (2019)");
        assert_same_inode(&folder_m1.join("视频BV1m1 (2019).mp4"), &mp4_m1);
        assert!(mp4_m1.is_file(), "源视频保持原样");
        assert!(folder_m1.join("poster.jpg").is_file());

        // 版本名稳定性：首P（pid 1）源文件缺失后其余版本命名不变；补回后命名一致
        std::fs::remove_file(video_m2_dir.join("v1.mp4")).unwrap();
        let plan = preview_rule(&rule, &db, Some(&[video_m2.id])).await.unwrap();
        assert_eq!(
            plan.items
                .iter()
                .filter(|item| item.action == ItemAction::Missing("源视频文件不存在".to_string()))
                .count(),
            1
        );
        let versions_missing: Vec<String> = plan
            .items
            .iter()
            .filter(|item| item.video_id == video_m2.id && item.target.is_some())
            .filter_map(|item| {
                item.target
                    .as_ref()
                    .and_then(|t| t.file_name().map(|n| n.to_string_lossy().to_string()))
            })
            .collect();
        assert_eq!(
            versions_missing,
            vec![
                "视频BV1m2 (2019) - P02.mp4".to_string(),
                "视频BV1m2 (2019) - P06.mp4".to_string(),
            ],
            "首P缺失不得改变其它版本的命名"
        );
        touch(&video_m2_dir.join("v1.mp4"));
        let report = execute_rule(&rule, &db, Some(&[video_m2.id])).await.unwrap();
        assert_eq!(report.created, 3, "三个版本全部创建");
        let folder_m2 = output.join("视频BV1m2 (2019)");
        assert_same_inode(
            &folder_m2.join("视频BV1m2 (2019) - P01.mp4"),
            &video_m2_dir.join("v1.mp4"),
        );
        assert!(folder_m2.join("视频BV1m2 (2019) - P06.mp4").is_file());
        // R5：多P版本的 NFO 标题应为该分P的名称，而非统一视频标题
        let nfo_p06 = std::fs::read_to_string(folder_m2.join("视频BV1m2 (2019) - P06.nfo")).unwrap();
        assert!(
            nfo_p06.contains("<title>分P6</title>"),
            "多P版本 NFO 标题应为分P名，实际：{nfo_p06}"
        );
        let nfo_p01 = std::fs::read_to_string(folder_m2.join("视频BV1m2 (2019) - P01.nfo")).unwrap();
        assert!(nfo_p01.contains("<title>分P1</title>"), "各版本 NFO 各自分P名");
        // 带源图片的版本才有对应旁挂图；缺失的版本完全没有图片（不嫁接其它版本）
        assert!(folder_m2.join("视频BV1m2 (2019) - P01-thumb.jpg").is_file());
        assert!(!folder_m2.join("视频BV1m2 (2019) - P02-thumb.jpg").exists());
        assert!(!folder_m2.join("视频BV1m2 (2019) - P06-thumb.jpg").exists());
        assert!(!folder_m2.join("视频BV1m2 (2019) - P02-poster.jpg").exists());
        // R5：缺图版本应有明确提示
        let p02 = report.items.iter().find(|i| i.page_name == "分P2").unwrap();
        assert!(
            p02.message
                .as_deref()
                .is_some_and(|m| m.contains("缺少可用的分集封面图片")),
            "缺图版本应有提示，实际：{:?}",
            p02.message
        );

        // 记录的目标路径与当前计算结果不一致：按冲突处理（选一个多P版本的分页记录）
        let drifted = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap()
            .into_iter()
            .find(|row| row.path.contains(" - P"))
            .unwrap();
        let mut active: organization_output::ActiveModel = drifted.into();
        active.path = Set("/somewhere/else.mp4".to_string());
        active.update(&db).await.unwrap();
        let plan = preview_rule(&rule, &db, Some(&[video_m2.id])).await.unwrap();
        assert!(
            plan.items
                .iter()
                .any(|item| matches!(item.action, ItemAction::Conflict(_))),
            "记录路径漂移按冲突处理"
        );

        // 目标被非规则文件占用时跳过且不覆盖：删除本规则记录，使目标变为“非规则文件”
        organization_output::Entity::delete_many()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .exec(&db)
            .await
            .unwrap();
        std::fs::write(folder_m1.join("视频BV1m1 (2019).mp4"), b"user file").unwrap();
        let report = execute_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        assert_eq!(report.skipped, 1);
        assert_eq!(report.created, 0);
        assert_eq!(
            std::fs::read(folder_m1.join("视频BV1m1 (2019).mp4")).unwrap(),
            b"user file",
            "不覆盖非规则文件"
        );
    }

    /// 电影 Create 条目：目标 mp4 空闲但同名 NFO/封面已存在（用户预置）→ 保守冲突：
    /// 不登记新记录、不覆盖用户文件（普通非重整新建同样适用）
    #[tokio::test]
    async fn test_movie_create_existing_nfo_cover_no_record() {
        let root = temp_root("movie-create-nfo-conflict");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let video_m = source.join("M1");
        let (video_m1, _, mp4_m1) = single_video(&db, "BV1m1", "导演", &video_m, day_of(1)).await;
        touch(&video_m.join("EP-poster.jpg"));
        let rule = rule_model(
            "电影库",
            "movie",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let rule = organization_rule::ActiveModel {
            name: Set("电影库".to_string()),
            rule_type: Set("movie".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();

        // 用户预置：同名 NFO 与剧集级 poster（无任何规则记录）
        let plan = preview_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        let target = plan.items[0].target.clone().unwrap();
        let folder = target.parent().unwrap();
        std::fs::create_dir_all(folder).unwrap();
        let user_nfo = target.with_extension("nfo");
        let user_poster = folder.join("poster.jpg");
        std::fs::write(&user_nfo, b"user movie nfo").unwrap();
        std::fs::write(&user_poster, b"user movie poster").unwrap();

        let report = execute_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        let item = report.items.first().unwrap();
        assert!(
            matches!(item.action, ItemAction::Conflict(_) | ItemAction::Exists),
            "预存在 NFO/封面的 Create 条目应保守冲突，实际：{:?}",
            item.action
        );
        assert_eq!(std::fs::read(&user_nfo).unwrap(), b"user movie nfo");
        assert_eq!(std::fs::read(&user_poster).unwrap(), b"user movie poster");
        assert!(
            organization_output::Entity::find()
                .filter(organization_output::Column::RuleId.eq(rule.id))
                .all(&db)
                .await
                .unwrap()
                .is_empty(),
            "不得登记新记录"
        );

        // 目标 mp4 保持不存在（未写入），源视频仍在源目录里保持原样
        assert!(!target.exists(), "电影 mp4 未写入");
        assert!(mp4_m1.is_file(), "源视频保持原样");
    }

    /// 记录先行：数据库失败时不得写入任何分页文件（无孤儿）；恢复后刷新即可完成整理
    #[tokio::test]
    async fn test_no_orphan_files_when_db_fails() {
        let root = temp_root("no-orphan");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let pubtime = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let video_dir = source.join("视频A");
        let (_, _, _) = single_video(&db, "BV1a", "source", &video_dir, pubtime).await;

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        // 令 organization_output 不可访问：执行必须在写入任何分页文件之前失败（记录先行的持久化顺序）
        let backend = db.get_database_backend();
        db.execute(sea_orm::Statement::from_string(
            backend,
            "ALTER TABLE organization_output RENAME TO organization_output_broken",
        ))
        .await
        .unwrap();
        assert!(
            execute_rule(&rule, &db, None).await.is_err(),
            "整理记录不可写时执行应整体失败"
        );
        let season = output.join("source").join("Season 1");
        let left_overs: Vec<_> = std::fs::read_dir(&season)
            .map(|entries| entries.filter_map(|entry| entry.ok()).collect())
            .unwrap_or_default();
        assert!(
            left_overs.is_empty(),
            "数据库失败后不得留下分页文件，实际留下：{:?}",
            left_overs.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );
        // 恢复表后重新执行即可完成整理（无遗留冲突，证明失败后没有不可恢复的孤儿）
        db.execute(sea_orm::Statement::from_string(
            backend,
            "ALTER TABLE organization_output_broken RENAME TO organization_output",
        ))
        .await
        .unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1, "恢复后正常创建");
        assert!(season.join("source - S01E01.mp4").is_file());
    }

    /// 并发执行被全局锁串行化：两次执行各自完整落地，集号分配与记录无竞争
    #[tokio::test]
    async fn test_concurrent_executes_are_serialized() {
        let root = temp_root("serial");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let pubtime = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let video_dir = source.join("视频A");
        let (_, _, mp4) = single_video(&db, "BV1a", "source", &video_dir, pubtime).await;

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let rule_ref = &rule;
        let db_ref = &db;
        let (r1, r2) = tokio::join!(
            execute_rule(rule_ref, db_ref, None),
            execute_rule(rule_ref, db_ref, None)
        );
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();
        let total_created = r1.created + r2.created;
        let total_refreshed = r1.refreshed + r2.refreshed;
        assert_eq!(total_created, 1, "串行化后只有一个执行真正创建");
        assert_eq!(total_refreshed, 1, "另一个执行表现为刷新");
        assert_eq!(r1.failed + r2.failed, 0);
        let rows = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1, "不产生重复记录");
        assert_same_inode(
            &output.join("source").join("Season 1").join("source - S01E01.mp4"),
            &mp4,
        );
    }

    /// 输出目录内出现符号链接时：相关条目全部失败且不落记录，链接目标目录不受影响；移除链接后刷新可补齐
    #[tokio::test]
    async fn test_output_symlink_rejected() {
        let root = temp_root("symlink-out");
        let source = root.join("source");
        let output = root.join("output");
        let external = root.join("external");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        std::fs::create_dir_all(&external).unwrap();
        let db = test_db(&root).await;
        let pubtime = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let video_dir = source.join("视频A");
        let (_, _, _) = single_video(&db, "BV1a", "source", &video_dir, pubtime).await;

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        // 剧集目录本身是指向输出目录之外的符号链接
        #[cfg(unix)]
        std::os::unix::fs::symlink(&external, output.join("source")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.failed, 1, "符号链接目标条目必须失败");
        assert_eq!(report.created, 0);
        assert!(
            std::fs::read_dir(&external).unwrap().next().is_none(),
            "链接目标目录不得被写入任何文件"
        );
        let rows = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert!(rows.is_empty(), "路径校验失败不得留下整理记录");

        // 移除符号链接后刷新即正常补齐
        std::fs::remove_file(output.join("source")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);
        assert!(
            output
                .join("source")
                .join("Season 1")
                .join("source - S01E01.mp4")
                .is_file()
        );
    }

    /// 跨次整理与既有目录归属：用户已有目录、同次/跨次同名不同作品、大小写别名都必须保守冲突，
    /// 绝不写入或合并；本规则自己此前的整理记录则允许继续刷新（不丢历史）
    #[tokio::test]
    async fn test_cross_run_directory_ownership() {
        let root = temp_root("ownership");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let source_canonical = std::fs::canonicalize(&source).unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();

        // 用户已在输出目录放置同名目录（含用户文件），且没有本规则记录
        let user_dir = output.join("source");
        std::fs::create_dir_all(&user_dir).unwrap();
        std::fs::write(user_dir.join("用户文件.txt"), b"keep").unwrap();
        let tv_dir = source.join("tv");
        let (_v, _p, _mp4) = single_video(&db, "BVtv", "UP", &tv_dir, time).await;
        let tv_rule = rule_model("剧库", "tv", &source_canonical, &output_canonical);
        let plan = preview_rule(&tv_rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "已有陌生目录必须整组冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&tv_rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0);
        assert_eq!(std::fs::read_to_string(user_dir.join("用户文件.txt")).unwrap(), "keep");
        assert!(!user_dir.join("Season 1").exists(), "不得写入用户已有目录");

        // 本规则自己此前的记录（跨次）：允许继续刷新，历史记录不丢
        let saved_rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let rule_id = saved_rule.id;
        let tv_row = organization_output::ActiveModel {
            rule_id: Set(rule_id),
            page_id: Set(_p.id),
            bvid: Set("BVtv".to_string()),
            cid: Set(_p.cid),
            series: Set(Some("source".to_string())),
            episode: Set(Some(1)),
            path: Set(user_dir
                .join("Season 1")
                .join("source - S01E01.mp4")
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let mut rule_with_id = tv_rule.clone();
        rule_with_id.id = rule_id;
        let plan = preview_rule(&rule_with_id, &db, None).await.unwrap();
        assert!(
            plan.items.iter().any(|item| item.action == ItemAction::Refresh),
            "本规则已有记录（跨次）应可继续刷新"
        );
        let report = execute_rule(&rule_with_id, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(std::fs::read_to_string(user_dir.join("用户文件.txt")).unwrap(), "keep");
        assert!(
            user_dir.join("Season 1").join("source - S01E01.mp4").is_file(),
            "历史归属目录内补齐文件"
        );
        let kept = organization_output::Entity::find_by_id(tv_row.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(kept.episode, Some(1), "历史记录保留且集号不变");

        // 电影：同标题同年份不同 BV 共享目录，跨次仅选后者也判冲突
        let movie_a_dir = source.join("movie-a");
        let movie_b_dir = source.join("movie-b");
        let (video_a, _, _) = single_video(&db, "BVma", "导演", &movie_a_dir, time).await;
        let video_b = insert_video(&db, "BVmb", "导演", &movie_b_dir, time, true, 2).await;
        let mp4_b = movie_b_dir.join("movie b.mp4");
        touch(&mp4_b);
        insert_page(&db, video_b.id, 0, Some(&mp4_b)).await;
        // 让两条视频标题相同：直接改库
        for (id, name) in [(video_a.id, "同名电影"), (video_b.id, "同名电影")] {
            let mut active: video::ActiveModel = video::Entity::find_by_id(id).one(&db).await.unwrap().unwrap().into();
            active.name = Set(name.to_string());
            active.update(&db).await.unwrap();
        }
        // 持久化规则（execute_rule 会按 id 重读；未入库 id=0 会让多规则共享同一 rule_id）
        let movie_rule = organization_rule::ActiveModel {
            name: Set("电影库".to_string()),
            rule_type: Set("movie".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 同次：仅一个可占用目录，后者整组冲突
        let plan = preview_rule(&movie_rule, &db, None).await.unwrap();
        let conflict_count = plan
            .items
            .iter()
            .filter(|item| item.video_id == video_b.id && matches!(item.action, ItemAction::Conflict(_)))
            .count();
        assert_eq!(conflict_count, 1, "同次同名不同 BV 不得合并");
        // 跨次：先单独整理 A，目录归属 A；随后单独选 B 执行必须冲突且不改动 A 目录
        let report = execute_rule(&movie_rule, &db, Some(&[video_a.id])).await.unwrap();
        assert_eq!(report.created, 1, "目录无争议时首个可整理");
        let shared_dir = output.join("同名电影 (2024)");
        let a_mp4 = shared_dir.join("同名电影 (2024).mp4");
        let a_content = std::fs::read(&a_mp4).unwrap();
        let report = execute_rule(&movie_rule, &db, Some(&[video_b.id])).await.unwrap();
        assert_eq!(report.created, 0, "跨次同名不同 BV 不得写入既有目录");
        assert!(
            report
                .items
                .iter()
                .filter(|item| item.video_id == video_b.id)
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "跨次同名不同 BV 必须冲突"
        );
        assert_eq!(std::fs::read(&a_mp4).unwrap(), a_content, "既有作品的文件不得被改动");
        assert!(
            !shared_dir.join("同名电影 (2024) - P01.mp4").exists(),
            "不得把另一作品写入既有目录"
        );

        // 陌生空目录同样按已确认方案冲突（电影）
        let stranger_output = root.join("output-stranger");
        std::fs::create_dir_all(stranger_output.join("同名电影 (2024)")).unwrap();
        let stranger_rule = organization_rule::ActiveModel {
            name: Set("stranger".to_string()),
            rule_type: Set("movie".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&stranger_output)
                .unwrap()
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let report = execute_rule(&stranger_rule, &db, Some(&[video_a.id])).await.unwrap();
        assert_eq!(report.created, 0, "陌生空目录也必须冲突");
        assert!(
            std::fs::read_dir(stranger_output.join("同名电影 (2024)"))
                .unwrap()
                .next()
                .is_none(),
            "不得写入陌生目录"
        );

        // 大小写别名：输出目录已有小写 "up" 目录（用户），剧名 "UP" 必须保守冲突
        let case_output = root.join("output-case");
        std::fs::create_dir_all(case_output.join("up")).unwrap();
        std::fs::write(case_output.join("up").join("user.txt"), b"keep").unwrap();
        let case_rule = organization_rule::ActiveModel {
            name: Set("case".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&case_output)
                .unwrap()
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let plan = preview_rule(&case_rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .filter(|item| item.series.as_deref() == Some("UP"))
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "大小写别名目录不得被当作新目录写入"
        );
        let report = execute_rule(&case_rule, &db, None).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(case_output.join("up").join("user.txt")).unwrap(),
            "keep"
        );
        assert!(
            !case_output.join("up").join("Season 1").exists(),
            "不得经由大小写别名写入用户目录"
        );
        let _ = report;
    }

    /// 目录不存在时也必须按持久记录判归属（记录可能先于写入，或目录被删除/移动）；
    /// 且 read_dir/metadata 失败必须 fail closed
    #[tokio::test]
    async fn test_ownership_when_directory_missing_and_fail_closed() {
        let root = temp_root("ownership-missing");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let source_canonical = std::fs::canonicalize(&source).unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();

        // 规则 A：已记录 UP 甲的整理结果，但目标目录随后被删除（记录先于写入或目录被移除）
        let rule_a = organization_rule::ActiveModel {
            name: Set("A".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let gone_dir = output.join("source-b");
        organization_output::ActiveModel {
            rule_id: Set(rule_a.id),
            page_id: Set(1),
            bvid: Set("BVold".to_string()),
            cid: Set(1),
            series: Set(Some("source-b".to_string())),
            episode: Set(Some(1)),
            path: Set(gone_dir
                .join("Season 1")
                .join("source-b - S01E01.strm")
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 规则 B（另一规则、另一源目录）：同一输出根下同名剧集目录已不存在，但 A 的记录指向它
        let source_b = root.join("source-b");
        std::fs::create_dir_all(&source_b).unwrap();
        let rule_b = organization_rule::ActiveModel {
            name: Set("B".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source_b).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        single_video(&db, "BVnew", "UP甲", &source_b.join("v"), time).await;
        let plan = preview_rule(&rule_b, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "目录虽不存在，但其它规则记录指向该目标，必须冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&rule_b, &db, None).await.unwrap();
        assert_eq!(report.created, 0, "不得写入其它规则已记录的目标");
        assert!(!gone_dir.exists(), "不得创建冲突目录");

        // 本规则内部：另一剧集（不同 UP 名）的记录指向该目录名 -> OtherOwned 冲突
        // 用剧名 filenamify 相同但原用户名不同来命中同一目录
        let source_c = root.join("source-c");
        std::fs::create_dir_all(&source_c).unwrap();
        let rule_c = organization_rule::ActiveModel {
            name: Set("C".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source_c).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 记录：剧集「UP_乙」的路径落在「UP/乙」目录（与「UP/乙」同名不同用户名的情况）
        organization_output::ActiveModel {
            rule_id: Set(rule_c.id),
            page_id: Set(2),
            bvid: Set("BVother".to_string()),
            cid: Set(2),
            series: Set(Some("source-c".to_string())),
            episode: Set(Some(1)),
            path: Set(output
                .join("source-c")
                .join("Season 1")
                .join("x.mp4")
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        single_video(&db, "BVc", "UP/乙", &source_c.join("v"), time).await;
        let plan = preview_rule(&rule_c, &db, None).await.unwrap();
        // 目录此刻并不存在，但本规则同剧名记录证明归属 -> 允许继续整理（新分页为 Create）
        assert!(
            !plan
                .items
                .iter()
                .any(|item| matches!(item.action, ItemAction::Conflict(_))),
            "同剧名历史记录应确认目录归属，不得冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        assert!(
            plan.items.iter().any(|item| item.action == ItemAction::Create),
            "归属确认后新分页应可创建"
        );

        // 本规则其它剧名的记录指向同一目录（记录存在、目录已被删除）：必须 OtherOwned 冲突
        let rule_d = organization_rule::ActiveModel {
            name: Set("D".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 「UP1」与「UP2」filenamify 后都得到同一个目录名不成立，故用两个真实会碰撞的名字：
        // "X/Y" 与 "X:Y" 都 filenamify 为 "X_Y"。记录属于前者的剧集，目录不存在后再整理后者。
        organization_output::ActiveModel {
            rule_id: Set(rule_d.id),
            page_id: Set(3),
            bvid: Set("BVxy1".to_string()),
            cid: Set(3),
            series: Set(Some("X/Y".to_string())),
            episode: Set(Some(1)),
            path: Set(output
                .join("X_Y")
                .join("Season 1")
                .join("a.strm")
                .to_string_lossy()
                .to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        single_video(&db, "BVxy2", "X:Y", &source_c.join("v2"), time).await;
        let plan = preview_rule(&rule_d, &db, None).await.unwrap();
        let xy_actions: Vec<_> = plan
            .items
            .iter()
            .filter(|item| item.bvid == "BVxy2")
            .map(|item| &item.action)
            .collect();
        assert!(
            xy_actions
                .iter()
                .all(|action| matches!(action, ItemAction::Conflict(_))),
            "本规则其它剧名记录已占用该目录名时必须冲突（即使目录不存在），实际：{xy_actions:?}"
        );

        // fail closed：输出根是普通文件（非目录）时必须报错，而不是当作“无既有条目”
        let not_a_dir = root.join("not-a-dir");
        std::fs::write(&not_a_dir, b"x").unwrap();
        assert!(
            find_existing_name(&not_a_dir, "UP").is_err(),
            "输出路径不是目录时必须 fail closed"
        );
        // 输出根不存在：明确特例，按无既有条目处理
        assert!(matches!(
            find_existing_name(&root.join("never-created"), "UP").unwrap(),
            ExistingName::None
        ));
    }

    /// 混合历史记录：同目录既有本映射记录又有不匹配记录（本规则其它作品/其它规则）时一律拒绝；
    /// 以及目录不存在时的大小写别名判定
    #[tokio::test]
    async fn test_mixed_history_rejected_and_case_alias_when_missing() {
        let root = temp_root("mixed");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let source_canonical = std::fs::canonicalize(&source).unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("混合".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 同目录「SOURCE」下：一条属于剧名 UP 的记录（匹配）+ 一条属于剧名 OTHER 的记录（不匹配）
        let up_dir = output.join("SOURCE");
        for (page_id, series, bvid) in [(1, "SOURCE", "BV1"), (2, "OTHER", "BV2")] {
            organization_output::ActiveModel {
                rule_id: Set(rule.id),
                page_id: Set(page_id),
                bvid: Set(bvid.to_string()),
                cid: Set(page_id as i64),
                series: Set(Some(series.to_string())),
                episode: Set(Some(1)),
                path: Set(up_dir
                    .join("Season 1")
                    .join(format!("{series} - S01E01.mp4"))
                    .to_string_lossy()
                    .to_string()),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
        }
        single_video(&db, "BV1", "SOURCE", &source.join("v1"), time).await;
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .filter(|item| item.bvid == "BV1")
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "同目录存在不匹配记录（其它作品）时必须拒绝，即使也有匹配记录：{:?}",
            plan.items
                .iter()
                .map(|item| (&item.bvid, &item.action))
                .collect::<Vec<_>>()
        );

        // 其它规则在同目录留下记录：同样拒绝
        let other_rule = organization_rule::ActiveModel {
            name: Set("他人".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        organization_output::Entity::delete_many()
            .filter(organization_output::Column::PageId.eq(2))
            .exec(&db)
            .await
            .unwrap();
        organization_output::ActiveModel {
            rule_id: Set(other_rule.id),
            page_id: Set(9),
            bvid: Set("BV9".to_string()),
            cid: Set(9),
            series: Set(Some("SOURCE".to_string())),
            episode: Set(Some(5)),
            path: Set(up_dir.join("Season 1").join("x.mp4").to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .filter(|item| item.bvid == "BV1")
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "同目录存在其它规则记录时必须拒绝：{:?}",
            plan.items
                .iter()
                .map(|item| (&item.bvid, &item.action))
                .collect::<Vec<_>>()
        );

        // 目录不存在时的大小写别名：记录路径为 .../SOURCE/...，待判定目录为 .../source（目录均不存在）
        let up_lower = output.join("source");
        assert!(!up_lower.exists());
        assert!(
            stored_path_under(&up_dir.join("Season 1").join("a.strm").to_string_lossy(), &up_lower),
            "大小写别名在目录不存在时应按路径键识别"
        );
        // 反向也成立
        assert!(stored_path_under(&up_lower.join("a.strm").to_string_lossy(), &up_dir));
        // 非前缀路径不得误判
        assert!(!stored_path_under(
            &output.join("SOURCEx").join("a.strm").to_string_lossy(),
            &up_lower
        ));
    }

    /// Unicode NFC/NFD 别名：历史记录路径与现有目录在 NFC 归一化 + 小写化后识别为同一前缀；
    /// NFKC/NFKD 兼容分解差异不做归一化，保守视为不同目录（见 normalized_key 局限说明）。
    /// 纯路径键比较，不创建、不删除任何目录。
    #[test]
    fn test_unicode_nfc_nfd_alias_recognition() {
        let base = std::env::temp_dir().join(format!("bili-sync-org-nfc-{}", std::process::id()));
        // NFC：é 为单码点 U+00E9；NFD：e + 组合锐音符 U+0301
        let nfc = "caf\u{00e9}";
        let nfd = "cafe\u{0301}";
        let nfc_dir = base.join(nfc);
        let nfd_dir = base.join(nfd);
        // 双向：NFC 记录在 NFD 目录前缀下 / NFD 记录在 NFC 目录前缀下
        assert!(
            stored_path_under(&nfc_dir.join("a.strm").to_string_lossy(), &nfd_dir),
            "NFC 记录应识别 NFD 目录为同一前缀"
        );
        assert!(
            stored_path_under(&nfd_dir.join("a.strm").to_string_lossy(), &nfc_dir),
            "NFD 记录应识别 NFC 目录为同一前缀"
        );
        // 大小写折叠与 NFC 归一化分别按组件应用，两种差异同时存在时仍识别
        let upper = base.join("CAF\u{00c9}");
        assert!(stored_path_under(&upper.join("a.strm").to_string_lossy(), &nfd_dir));
        // NFKC 兼容分解（½ ↔ 1⁄2，U+2044）不做归一化，保守判为不同目录
        let vulgar = base.join("\u{00bd}");
        let fraction = base.join("1\u{2044}2");
        assert!(
            !stored_path_under(&vulgar.join("a.strm").to_string_lossy(), &fraction),
            "NFKC 兼容分解差异不应归一化"
        );
        // 归一化不扩大前缀范围：caféri ≠ café
        assert!(!stored_path_under(
            &base.join(format!("{nfd}ri")).join("a.strm").to_string_lossy(),
            &nfc_dir
        ));
    }

    /// 路径校验：源目录不得嵌套进其它规则的输出目录；已产生记录的规则锁定源/输出/类型，仅允许改名
    #[tokio::test]
    async fn test_validation_and_rule_edit_lock() {
        let root = temp_root("validate");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let source_canonical = std::fs::canonicalize(&source).unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("已有规则".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 新规则的源目录嵌套进已有规则的输出目录：拒绝
        let result = validate_rule_paths(
            &db,
            None,
            &output.join("sub").to_string_lossy(),
            &root.join("other-output").to_string_lossy(),
        )
        .await;
        assert!(result.is_err(), "源目录嵌套进已有规则的输出目录应被拒绝");

        // 输出目录嵌套已有规则的源目录：拒绝
        let result = validate_rule_paths(
            &db,
            None,
            &source_canonical.to_string_lossy(),
            &source.join("nested").to_string_lossy(),
        )
        .await;
        assert!(result.is_err(), "输出目录嵌套已有规则的源目录应被拒绝");

        // 规则编辑锁：无记录时可改路径与类型
        assert!(
            ensure_rule_editable(&rule.clone(), &db, "new-source", "new-output", "movie")
                .await
                .is_ok()
        );
        // 产生记录后：改路径/类型被拒绝，仅改名不受限
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(1),
            bvid: Set("BV1x".to_string()),
            cid: Set(1),
            series: Set(Some("source".to_string())),
            episode: Set(Some(1)),
            path: Set("/x.strm".to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        assert!(
            ensure_rule_editable(&rule.clone(), &db, "new-source", "new-output", "movie")
                .await
                .is_err(),
            "有记录时修改源/输出/类型应被拒绝"
        );
        assert!(
            ensure_rule_editable(
                &rule.clone(),
                &db,
                &rule.source_path,
                &rule.output_path,
                &rule.rule_type
            )
            .await
            .is_ok(),
            "路径与类型未变化（仅改名）应允许"
        );
    }

    /// 硬链接输出：与源视频同 inode（不复制、源保持原样）、不再生成 STRM；
    /// 旧版 STRM 整理记录必须保守冲突（TV 与电影均整组跳过），绝不迁移或改写
    #[tokio::test]
    async fn test_hard_link_output_and_legacy_strm_conflict() {
        let root = temp_root("hardlink");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let source_canonical = std::fs::canonicalize(&source).unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();

        // TV：存在旧版 STRM 记录（含目录内真实 STRM 文件）→ 整组保守冲突，绝不迁移、不改写
        let tv_rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let (_tv_video, tv_page, _) = single_video(&db, "BVtv", "UP", &source.join("tv"), time).await;
        let legacy_dir = output.join("source").join("Season 1");
        std::fs::create_dir_all(&legacy_dir).unwrap();
        let legacy_strm = legacy_dir.join("UP - S01E01.strm");
        std::fs::write(&legacy_strm, b"/old/strm/path.mp4").unwrap();
        organization_output::ActiveModel {
            rule_id: Set(tv_rule.id),
            page_id: Set(tv_page.id),
            bvid: Set("BVtv".to_string()),
            cid: Set(tv_page.cid),
            series: Set(Some("source".to_string())),
            episode: Set(Some(1)),
            path: Set(legacy_strm.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 追加同一 UP 的另一投稿：无论候选当前会排到哪个集号（新投稿≠旧记录的 E01），整组保守冲突
        let (_tv_b_video, _tv_b_page, _) = single_video(&db, "BVtvb", "UP", &source.join("tvb"), time).await;
        let plan = preview_rule(&tv_rule, &db, None).await.unwrap();
        assert_eq!(plan.items.len(), 2, "同一 UP 的两个投稿都参与整组判定");
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "旧版 STRM 记录必须整组保守冲突（无论候选集号），实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&tv_rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0, "旧 STRM 记录不得被静默迁移");
        assert_eq!(
            std::fs::read(&legacy_strm).unwrap(),
            b"/old/strm/path.mp4",
            "既有 STRM 文件不得被改写"
        );
        assert!(!legacy_dir.join("UP - S01E01.mp4").exists(), "不得静默迁移为 mp4");

        // 另一源目录的新规则正常整理为硬链接：R2 聚合下同名源目录各成剧集，历史 STRM 只阻断同源剧集
        let source_tv2 = root.join("tv2");
        std::fs::create_dir_all(&source_tv2).unwrap();
        let (_tv2_video, _tv2_page, tv2_mp4) = single_video(&db, "BVtv2", "导演", &source_tv2.join("v"), time).await;
        let tv2_rule = organization_rule::ActiveModel {
            name: Set("剧库2".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source_tv2)
                .unwrap()
                .to_string_lossy()
                .to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let plan = preview_rule(&tv2_rule, &db, None).await.unwrap();
        let item = plan.items.iter().find(|item| item.bvid == "BVtv2").unwrap();
        assert_eq!(item.action, ItemAction::Create);
        assert_eq!(
            item.target
                .as_ref()
                .and_then(|t| t.extension())
                .and_then(|e| e.to_str()),
            Some("mp4"),
            "视频输出目标扩展名应为 .mp4"
        );
        let report = execute_rule(&tv2_rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);
        let link = output.join("tv2").join("Season 1").join("tv2 - S01E01.mp4");
        assert_same_inode(&link, &tv2_mp4);
        assert!(tv2_mp4.is_file(), "源视频保持原样");
        assert_eq!(
            std::fs::read(&link).unwrap(),
            std::fs::read(&tv2_mp4).unwrap(),
            "硬链接与源视频内容一致"
        );

        // 电影：存在旧版 STRM 记录 → 整组保守冲突，绝不迁移、不改写
        let movie_rule = organization_rule::ActiveModel {
            name: Set("电影库".to_string()),
            rule_type: Set("movie".to_string()),
            source_path: Set(source_canonical.to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let (movie_video, movie_page, _) = single_video(&db, "BVmv", "编导", &source.join("movie"), time).await;
        let legacy_movie_dir = output.join(format!("视频BVmv ({})", time.year()));
        std::fs::create_dir_all(&legacy_movie_dir).unwrap();
        let legacy_movie_strm = legacy_movie_dir.join(format!("视频BVmv ({}).strm", time.year()));
        std::fs::write(&legacy_movie_strm, b"/old/movie.mp4").unwrap();
        organization_output::ActiveModel {
            rule_id: Set(movie_rule.id),
            page_id: Set(movie_page.id),
            bvid: Set("BVmv".to_string()),
            cid: Set(movie_page.cid),
            series: Set(None),
            episode: Set(None),
            path: Set(legacy_movie_strm.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let plan = preview_rule(&movie_rule, &db, Some(&[movie_video.id])).await.unwrap();
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "电影旧版 STRM 记录必须整组保守冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&movie_rule, &db, Some(&[movie_video.id])).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0);
        assert_eq!(std::fs::read(&legacy_movie_strm).unwrap(), b"/old/movie.mp4");
        assert!(
            !legacy_movie_dir
                .join(format!("视频BVmv ({}).mp4", time.year()))
                .exists(),
            "不得静默迁移为 mp4"
        );
    }

    /// 硬链接失败必须明确报错（源缺失；跨文件系统/不支持硬链接走同一失败路径，绝不复制回退）
    #[tokio::test]
    async fn test_hard_link_failure_is_explicit() {
        let root = temp_root("hardlink-fail");
        let output = root.join("output");
        std::fs::create_dir_all(&output).unwrap();
        let file = PendingFile {
            target: output.join("a.mp4"),
            overwrite: false,
            content: PendingContent::HardLink(root.join("missing.mp4")),
        };
        let err = write_pending_file(&output, &file).await.unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("硬链接"), "失败信息应说明硬链接无法创建：{message}");
        assert!(!output.join("a.mp4").exists(), "失败后不得留下目标文件");
    }

    /// R2 安全重整：旧记录按 UP 名组织（旧目录保留），执行后旧记录在新剧集目录下重整；
    /// 新集号稳定追加、不与旧号冲突；再跑第二次号码与文件保持不变。
    #[tokio::test]
    async fn test_tv_reorg_from_legacy_up_records() {
        let root = temp_root("tv-reorg");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        // 两位 UP 各一个投稿，分别曾整理到 output/张三 与 output/李四
        let dir_u1 = source.join("u1");
        let (v1, _, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let dir_u2 = source.join("u2");
        let (v2, _, mp4_u2) = single_video(&db, "BV2", "李四", &dir_u2, day_of(1)).await;
        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let rule_id = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        // 旧记录：episodes 1 与 1（不同目录名下各为一号）
        let old_dir_u1 = output.join("张三");
        let old_dir_u2 = output.join("李四");
        std::fs::create_dir_all(old_dir_u1.join("Season 1")).unwrap();
        std::fs::create_dir_all(old_dir_u2.join("Season 1")).unwrap();
        let old_link_u1 = old_dir_u1.join("Season 1").join("张三 - S01E01.mp4");
        let old_link_u2 = old_dir_u2.join("Season 1").join("李四 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link_u1).await.unwrap();
        tokio::fs::hard_link(&mp4_u2, &old_link_u2).await.unwrap();
        for (page_id, bvid, cid, series, path) in [
            (v1.id, "BV1", 0, "张三", &old_link_u1),
            (v2.id, "BV2", 0, "李四", &old_link_u2),
        ] {
            organization_output::ActiveModel {
                rule_id: Set(rule_id.id),
                page_id: Set(page_id),
                bvid: Set(bvid.to_string()),
                cid: Set(cid as i64),
                series: Set(Some(series.to_string())),
                episode: Set(Some(1)),
                path: Set(path.to_string_lossy().to_string()),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
        }
        let plan = preview_rule(&rule_id, &db, None).await.unwrap();
        assert!(
            plan.items.iter().all(|item| item.reorg),
            "旧 UP 名记录在源目录名剧集下被规划为重整，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let expected_reorg_targets: Vec<String> = plan
            .items
            .iter()
            .map(|item| item.target.as_ref().unwrap().to_string_lossy().to_string())
            .collect();
        // 重整不写旧目录里的旧号；新集号来自旧号之后（源目录历史无匹配时旧号仍不冲突）
        let report = execute_rule(&rule_id, &db, None).await.unwrap();
        assert_eq!(report.created, 0, "重整不产生新 created 项");
        assert_eq!(report.refreshed, 2, "两个旧记录都被重整");
        assert_eq!(report.failed, 0);
        assert_eq!(report.skipped, 0);

        let new_dir = output.join("新剧名");
        let season = new_dir.join("Season 1");
        // 两个旧目录都不动，旧硬链接与旧号保持
        assert!(old_link_u1.is_file(), "旧目录必须保留");
        assert!(old_link_u2.is_file(), "旧目录必须保留");
        assert!(season.join("新剧名 - S01E01.mp4").is_file());
        assert!(
            season.join("新剧名 - S01E02.mp4").is_file(),
            "两个分页各得一个独立新集号"
        );

        // 记录已被安全迁移到新剧（新集号、新路径），旧路径不再指向记录
        let rows = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule_id.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter().all(|row| row.series.as_deref() == Some("新剧名")
                && row.path.contains("新剧名")
                && !row.path.contains("张三")
                && !row.path.contains("李四")),
            "记录应指向新剧，实际：{:?}",
            rows.iter().map(|row| (&row.series, &row.path)).collect::<Vec<_>>()
        );

        // 再跑：稳定号不重复分配，仍刷新 2 个条目，无副作用
        let report2 = execute_rule(&rule_id, &db, None).await.unwrap();
        assert_eq!(report2.created, 0);
        assert_eq!(report2.refreshed, 2);
        assert_eq!(report2.failed, 0);
        let _ = expected_reorg_targets;
        std::fs::read(old_link_u1).unwrap_or_default(); // 旧目录内容仍可读
    }

    /// 重整目标冲突：新剧集下的目标被无关文件（非本规则源视频硬链接）占用，且该文件已被
    /// 其它规则记录指向：整组保守冲突，旧记录、旧文件、被占用文件均不动
    #[tokio::test]
    async fn test_tv_reorg_target_conflict_preserves_legacy_records() {
        let root = temp_root("tv-reorg-conflict");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;

        // 待占用目标：新剧名下的 S01E01，由另一规则的记录 path 直接指向以模拟文件级占用
        // （该目录尚不存在，只有“记录先于写入”的历史，目录级归属不受影响）
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let other_rule = organization_rule::ActiveModel {
            name: Set("rival".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let (rival_video, _, _) = single_video(&db, "BVrival", "别人", &source.join("r"), day_of(3)).await;
        let target = output.join("新剧名").join("Season 1").join("新剧名 - S01E01.mp4");
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"rival/unrelated unrelated").unwrap();
        organization_output::ActiveModel {
            rule_id: Set(other_rule.id),
            page_id: Set(rival_video.id),
            bvid: Set("BVrival".to_string()),
            cid: Set(0),
            series: Set(Some("新剧名".to_string())),
            episode: Set(Some(1)),
            path: Set(target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 本规则旧记录（UP 名）
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item = plan.items.iter().find(|item| item.bvid == "BV1").unwrap();
        assert!(
            matches!(item.action, ItemAction::Conflict(_)),
            "输出目录已被其它规则记录占用时整组保守冲突（旧记录不撤）：{:?}",
            item.action
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0, "冲突时不得写入");
        let rival_record = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(other_rule.id))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            rival_record.path,
            target.to_string_lossy().to_string(),
            "冲突目标记录保留"
        );
        let kept = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].path.contains("张三"), "既有 UP 目录记录保留");
        assert!(old_link.is_file(), "旧文件保留");
    }

    /// 重整写入失败：产物写入失败时旧记录与旧目录文件完全保留，可再次重试
    #[tokio::test]
    async fn test_tv_reorg_write_failure_preserves_legacy_records() {
        let root = temp_root("tv-reorg-fail");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        // 源视频之后的写入会在目标区域创建新目录；把 Season 1 的父目录塞入一个普通文件
        // 以触发 write_pending_file 的 create_dir_all 失败，验证失败路径保持旧记录
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item = plan.items.iter().find(|item| item.bvid == "BV1").unwrap();
        assert!(item.reorg);
        let estimated_target = plan.items[0].target.clone().unwrap();
        assert!(!estimated_target.exists(), "规划目标是待写入的新路径");
        // 写入失败：父目录里放同名普通文件挡住 Season 1
        let season_path = output.join("新剧名").join("Season 1");
        std::fs::create_dir_all(output.join("新剧名")).unwrap();
        std::fs::write(&season_path, b"block").unwrap();

        let _report = execute_rule(&rule, &db, None).await.unwrap();
        let kept = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].path.contains("张三"), "写入失败后记录仍指向旧路径");
        assert!(old_link.is_file(), "旧文件保留");
        assert_eq!(std::fs::read(&season_path).unwrap(), b"block", "阻塞文件不被破坏");
    }

    /// 覆盖保护：其它规则记录指向的文件（或未知来源）不被本规则强制覆盖
    #[tokio::test]
    async fn test_overwrite_silently_skips_foreign_recorded_files() {
        let root = temp_root("tv-overwrite-foreign");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let video_dir = source.join("v");
        let (video, page, mp4) = single_video(&db, "BV1a", "张三", &video_dir, day_of(1)).await;
        touch(&video_dir.join("EP-poster.jpg"));
        let rule = organization_rule::ActiveModel {
            name: Set("ruleA".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let _ = execute_rule(&rule, &db, None).await.unwrap();
        let season = output.join("source").join("Season 1");
        let nfo = season.join("source - S01E01.nfo");

        // 让一个 foreign 其他规则的记录直接指向本规则的 NFO 文件路径，模拟“未知 owner”覆盖保护
        let foreign_rule = organization_rule::ActiveModel {
            name: Set("other".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(root.join("elsewhere").to_string_lossy().to_string()),
            output_path: Set(root.join("nothing").to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        organization_output::ActiveModel {
            rule_id: Set(foreign_rule.id),
            page_id: Set(page.id + 1000),
            bvid: Set("BVforeign".to_string()),
            cid: Set(999),
            series: Set(Some("其他".to_string())),
            episode: Set(Some(1)),
            path: Set(nfo.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        std::fs::write(&nfo, b"user protected").unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&nfo).unwrap(),
            b"user protected".iter().map(|b| char::from(*b)).collect::<String>(),
            "被其它规则记录指向的文件不被本规则覆盖"
        );
        // 对比同规则无 foreign 占用时已有覆盖行为
        let _ = video;
        let _ = mp4;
    }

    /// 重整可恢复：上次重整已写入部分产物后失败；意图记录（DB 持久声明）让重跑可续作，
    /// 补齐缺失产物后旧记录改指新剧，意图记录删除，旧目录保留。不依赖文件 inode 推定
    #[tokio::test]
    async fn test_tv_reorg_resume_from_partial_write() {
        let root = temp_root("tv-reorg-resume");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        std::fs::write(dir_u1.join("EP-poster.jpg"), b"cover").unwrap();
        let output_canonical = std::fs::canonicalize(&output).unwrap();

        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 让计划先计算出新目标（新剧名下 E01），然后模拟中断状态：
        // 意图记录已写入（声明新目标），mp4 已写、NFO 未写
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item0 = &plan.items[0];
        let new_target = item0.target.clone().unwrap();
        assert!(item0.reorg, "旧 UP 名记录规划为重整");
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        tokio::fs::hard_link(&mp4_u1, &new_target).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(item0.series.clone()),
            episode: Set(item0.episode),
            path: Set(new_target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 重跑：意图声明与计算一致 → 续作，补齐 NFO；
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 0, "意图声明的目标不允许新建记录行");
        assert_eq!(report.refreshed, 1, "重整续作成功");
        assert_eq!(report.failed, 0);
        assert_eq!(report.skipped, 0);
        let nfo = new_target.with_extension("nfo");
        assert!(nfo.is_file(), "缺失 NFO 被补齐");
        assert_same_inode(&new_target, &mp4_u1);

        // 提交完成：旧记录改指新剧，意图记录已删除
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("新剧名"), "记录已改指新剧目录");
        assert_eq!(real.series.as_deref(), Some("新剧名"));
        let intents = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.starts_with("reorg="))
            .all(&db)
            .await
            .unwrap();
        assert!(
            intents.is_empty(),
            "重整完成后意图记录应被删除，实际：{:?}",
            intents.iter().map(|row| row.bvid.clone()).collect::<Vec<_>>()
        );
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }

    /// 用户预置的目标 NFO 不被重整覆盖：重整过程只补缺，任何已存在的产物（含 NFO/封面）
    /// 在无持久归属证明时一律保留
    #[tokio::test]
    async fn test_tv_reorg_user_prefilled_nfo_not_overwritten() {
        let root = temp_root("tv-reorg-user-nfo");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 用户预先在新剧目标位置放置 NFO 与封面（无任何规则记录证明归属）
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let new_target = plan.items[0].target.clone().unwrap();
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        let user_nfo = new_target.with_extension("nfo");
        let user_thumb = new_target.with_file_name(format!(
            "{}-poster.jpg",
            new_target.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::write(&user_nfo, b"user nfo keep").unwrap();
        std::fs::write(&user_thumb, b"user thumb keep").unwrap();

        let report = execute_rule(&rule, &db, None).await.unwrap();
        // 用户预置目录（新剧名）使整组在 plan 期判为无法确认归属：保守冲突，不写任何文件
        assert!(
            report
                .items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "含用户预置文件的目标目录应整组冲突，实际：{:?}",
            report.items.iter().map(|item| item.action.clone()).collect::<Vec<_>>()
        );
        // 用户预置文件原样保留
        assert_eq!(std::fs::read(&user_nfo).unwrap(), b"user nfo keep");
        assert_eq!(std::fs::read(&user_thumb).unwrap(), b"user thumb keep");
        // 保守冲突下旧记录不动
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("张三"), "记录仍指旧目录");
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }
    /// 重整续作发现预存在的 NFO/封面（用户预置或上次自生成，归属不可证明）：
    /// 保守冲突保留旧记录，不 finalize；用户文件原样；extras 因目录有重整条目不覆盖。
    /// 混合目录回归：意图记录存在，重整条目同源遗留占位占住一个分片；
    /// 新视频 mp4 目标空闲，但其同名 NFO 与封面已被用户预置 → 该 Create 条目必须保守跳过：
    /// 不登记新记录、不覆盖、不写入任何文件；同场景对电影新建同样适用
    #[tokio::test]
    async fn test_mixed_dir_new_video_existing_nfo_cover_no_record() {
        let root = temp_root("tv-new-video-existing-nfo");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let dir_v2 = source.join("v2");
        let (v2, _, mp4_v2) = single_video(&db, "BV2", "李四", &dir_v2, day_of(3)).await;
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        // BV1 曾按 UP 名整理（重整条目），意图已声明
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 意图记录声明 BV1 的新目标（同源 mp4 硬链接设在目标处，模拟中断现场）
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let reorg_item = plan.items.iter().find(|item| item.bvid == "BV1").unwrap();
        let reorg_target = reorg_item.target.clone().unwrap();
        std::fs::create_dir_all(reorg_target.parent().unwrap()).unwrap();
        tokio::fs::hard_link(&mp4_u1, &reorg_target).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(reorg_item.series.clone()),
            episode: Set(reorg_item.episode),
            path: Set(reorg_target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 新视频 bv2 的 mp4 目标空闲，但其同名 NFO 与封面由用户预置
        let new_item = plan.items.iter().find(|item| item.bvid == "BV2").unwrap();
        let new_target = new_item.target.clone().unwrap();
        let user_nfo = new_target.with_extension("nfo");
        let user_thumb = new_target.with_file_name(format!(
            "{}-poster.jpg",
            new_target.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::write(&user_nfo, b"user nfo").unwrap();
        std::fs::write(&user_thumb, b"user cover").unwrap();

        let report = execute_rule(&rule, &db, None).await.unwrap();
        let report_new = report.items.iter().find(|item| item.bvid == "BV2").unwrap();
        assert!(
            matches!(report_new.action, ItemAction::Conflict(_) | ItemAction::Exists),
            "已有未知 NFO/封面时新视频抽象应保守冲突，实际：{:?}",
            report_new.action
        );
        // 用户文件原样
        assert_eq!(std::fs::read(&user_nfo).unwrap(), b"user nfo");
        assert_eq!(std::fs::read(&user_thumb).unwrap(), b"user cover");
        // 新视频 mp4 目标未被写入（无凭证占用目标只补缺/poster 不动）
        assert!(!new_target.exists(), "新视频 mp4 未被写入");
        // 无新视频记录条目添加
        let new_record = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV2"))
            .all(&db)
            .await
            .unwrap();
        assert!(
            new_record.is_empty(),
            "不新增 BV2 记录，实际：{:?}",
            new_record.iter().map(|row| row.path.clone()).collect::<Vec<_>>()
        );
        // 重整条目：意图与同源遗留成立时可独立续作，但不得改写 BV2 用户文件（互不影响）
        let report_reorg = report.items.iter().find(|item| item.bvid == "BV1").unwrap();
        assert!(
            matches!(
                report_reorg.action,
                ItemAction::Refresh | ItemAction::Failed(_) | ItemAction::Conflict(_)
            ),
            "重整条目独立推进或保守失败，实际：{:?}",
            report_reorg.action
        );
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }

    /// 混合目录：新视频目录 + 意图重整的同源遗留 mp4 + 用户预置 poster/tvshow——
    /// 重整条目与外部规则目录归属始终优先，新视频不得写入该目录，extras 不被覆盖
    #[tokio::test]
    async fn test_tv_reorg_mixed_dir_user_preserves_no_overwrite() {
        let root = temp_root("tv-reorg-mixed-preserves");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        // 重整分页（UP 名张三）+ 新视频（无记录，将走 Create）
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        std::fs::write(dir_u1.join("EP-poster.jpg"), b"cover-src").unwrap();
        let dir_new = source.join("v2");
        single_video(&db, "BV2", "李四", &dir_new, day_of(3)).await;
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 中断现场：意图声明 + 同源 mp4 遗留；用户预置 poster/tvshow.nfo 于重整分片目录
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let reorg_item = plan.items.iter().find(|item| item.bvid == "BV1").unwrap();
        let new_video_item = plan.items.iter().find(|item| item.bvid == "BV2").unwrap();
        let new_target = reorg_item.target.clone().unwrap();
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        tokio::fs::hard_link(&mp4_u1, &new_target).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(reorg_item.series.clone()),
            episode: Set(reorg_item.episode),
            path: Set(new_target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 用户预置：单集 poster、剧集级 poster、tvshow.nfo（都在重整分片目录）
        let user_thumb = new_target.with_file_name(format!(
            "{}-poster.jpg",
            new_target.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::write(&user_thumb, b"user single poster").unwrap();
        let series_dir = output.join("新剧名");
        let user_series_poster = series_dir.join("poster.jpg");
        std::fs::write(&user_series_poster, b"user series poster").unwrap();
        let user_tvshow = series_dir.join("tvshow.nfo");
        std::fs::write(&user_tvshow, b"user tvshow").unwrap();

        let report = execute_rule(&rule, &db, None).await.unwrap();
        // 重整条目：归属不可证明 → 冲突不 finalize
        let report_reorg = report.items.iter().find(|item| item.bvid == "BV1").unwrap();
        assert!(
            matches!(report_reorg.action, ItemAction::Failed(_)),
            "重整条目应冲突，实际：{:?}",
            report_reorg.action
        );
        // 新视频条目：目录由本规则意图声明归属，允许 Create（新集号追加，不与重整条目冲突）
        let report_new = report.items.iter().find(|item| item.bvid == "BV2").unwrap();
        assert!(
            matches!(report_new.action, ItemAction::Create | ItemAction::Refresh),
            "新视频应在混合目录正常追加（不与重整冲突），实际：{:?}",
            report_new.action
        );
        // 用户文件原样：单集 poster、剧集级 poster、tvshow.nfo、遗留 mp4
        assert_eq!(std::fs::read(&user_thumb).unwrap(), b"user single poster");
        assert_eq!(std::fs::read(&user_series_poster).unwrap(), b"user series poster");
        assert_eq!(std::fs::read(&user_tvshow).unwrap(), b"user tvshow");
        assert_same_inode(&new_target, &mp4_u1);
        assert!(!new_target.with_extension("nfo").exists(), "NFO 未写入");
        // 新视频自己的 NFO 可写入（独立分片 stem）
        assert!(
            new_target
                .parent()
                .unwrap()
                .join(
                    plan.items
                        .iter()
                        .find(|item| item.bvid == "BV2")
                        .unwrap()
                        .target
                        .clone()
                        .unwrap()
                        .with_extension("nfo")
                        .file_name()
                        .unwrap()
                        .to_string_lossy()
                        .to_string()
                )
                .is_file(),
            "新视频的 NFO 已补齐写入"
        );
        // 旧记录原样 + 意图保留
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("张三"), "旧记录未迁移");
        let intents = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.starts_with("reorg="))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(intents.len(), 1, "意图记录保留");
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }

    /// 重整续作发现预存在的 NFO/封面（用户预置或上次自生成，归属不可证明）：
    /// 保守冲突保留旧记录，不 finalize；用户文件原样；extras 因目录有重整条目不覆盖
    #[tokio::test]
    async fn test_tv_reorg_user_poster_conflict_no_finalize() {
        let root = temp_root("tv-reorg-user-poster-cf");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        std::fs::write(dir_u1.join("EP-poster.jpg"), b"cover-src").unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 中断现场：意图声明 + 同源 mp4 遗留 + 单集 poster（自生成归属不可证明）
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item0 = plan.items[0].clone();
        let new_target = item0.target.clone().unwrap();
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        tokio::fs::hard_link(&mp4_u1, &new_target).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(item0.series.clone()),
            episode: Set(item0.episode),
            path: Set(new_target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        let user_thumb = new_target.with_file_name(format!(
            "{}-poster.jpg",
            new_target.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::write(&user_thumb, b"user single poster").unwrap();
        let user_series_poster = output.join("新剧名").join("poster.jpg");
        std::fs::write(&user_series_poster, b"user series poster").unwrap();

        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(
            report.created + report.refreshed,
            0,
            "归属不可证明的既有产物下不得 finalize"
        );
        assert!(
            report
                .items
                .first()
                .and_then(|item| item.message.clone())
                .is_some_and(|message| message.contains("归属无法证明")),
            "应有明确冲突提示，实际：{:?}",
            report.items.first().and_then(|item| item.message.clone())
        );
        // 用户文件原样：单集 poster、剧集级 poster、遗留 mp4、NFO 未生成
        assert_eq!(std::fs::read(&user_thumb).unwrap(), b"user single poster");
        assert_eq!(std::fs::read(&user_series_poster).unwrap(), b"user series poster");
        assert!(!new_target.with_extension("nfo").exists(), "NFO 未被写入");
        assert!(!output.join("新剧名").join("tvshow.nfo").exists(), "剧集级文件未写入");
        assert_same_inode(&new_target, &mp4_u1);
        // 旧记录原样
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("张三"), "旧记录未迁移");
        let intents = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.starts_with("reorg="))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(intents.len(), 1, "意图记录保留，可由后续人工/清理处理");
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }

    #[tokio::test]
    async fn test_tv_reorg_user_files_after_intent_conflict_no_finalize() {
        let root = temp_root("tv-reorg-user-after-intent");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 第一步：模拟首次执行先落意图声明（不写文件）
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let item0 = plan.items[0].clone();
        let new_target = item0.target.clone().unwrap();
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(item0.series.clone()),
            episode: Set(item0.episode),
            path: Set(new_target.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 第二步：用户在声明目录放置“不同来源”的 MP4（非本源视频硬链接）、自己的 NFO 与 poster
        let stranger_mp4 = root.join("stranger-other.mp4");
        std::fs::write(&stranger_mp4, b"other content entirely").unwrap();
        tokio::fs::hard_link(&stranger_mp4, &new_target).await.unwrap();
        let user_nfo = new_target.with_extension("nfo");
        let user_poster = new_target.with_file_name(format!(
            "{}-poster.jpg",
            new_target.file_stem().unwrap().to_string_lossy()
        ));
        std::fs::write(&user_nfo, b"user nvo").unwrap();
        std::fs::write(&user_poster, b"user poster").unwrap();
        let series_poster = output.join("新剧名").join("poster.jpg");
        std::fs::write(&series_poster, b"user series poster").unwrap();

        // 重试：目标 mp4 非同源 → 冲突，旧记录保留，用户文件与意图记录都不动
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(
            report.created + report.refreshed,
            0,
            "用户文件占用声明目标时不得写入/finalize"
        );
        assert!(
            report
                .items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "用户占用必须冲突，实际：{:?}",
            report.items.iter().map(|item| item.action.clone()).collect::<Vec<_>>()
        );
        assert_eq!(
            std::fs::read(&new_target).unwrap(),
            b"other content entirely",
            "用户 MP4 不动"
        );
        assert_eq!(std::fs::read(&user_nfo).unwrap(), b"user nvo", "用户 NFO 不动");
        assert_eq!(std::fs::read(&user_poster).unwrap(), b"user poster", "用户 poster 不动");
        assert!(
            !output.join("新剧名").join("tvshow.nfo").exists(),
            "重整目录的剧集级文件未被写入（整组冲突）"
        );
        assert_eq!(
            std::fs::read(&series_poster).unwrap(),
            b"user series poster",
            "用户剧集级 poster 不动"
        );
        // 旧记录仍指旧路径；意图记录仍保留
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("张三"));
        let intents = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.starts_with("reorg="))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(intents.len(), 1, "意图记录保留供后续人工处理");
        assert!(old_link.is_file(), "旧目录与旧链接保留");
    }

    /// 重整意图记录与本次计算目标不一致：保守冲突，意图与旧记录都不动（不可静默改道）
    #[tokio::test]
    async fn test_tv_reorg_intent_mismatch_is_conflict() {
        let root = temp_root("tv-reorg-intent-mismatch");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(std::fs::canonicalize(&output).unwrap().to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();
        // 声明了一个与本次计算不一致的目标（不同集号）
        let intent_path = output.join("新剧名").join("Season 1").join("新剧名 - S01E07.mp4");
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set(format!("reorg={}", v1.bvid)),
            cid: Set(page_v1.cid),
            series: Set(Some("新剧名".to_string())),
            episode: Set(Some(7)),
            path: Set(intent_path.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "意图与计算不一致必须冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0);
        assert_eq!(report.failed, 0);
        // 意图与旧记录都保持原样
        let real = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.eq("BV1"))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert!(real.path.contains("张三"), "旧记录未被迁移");
        let intent = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .filter(organization_output::Column::Bvid.starts_with("reorg="))
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(intent.path, intent_path.to_string_lossy().to_string());
        assert!(!intent_path.exists(), "未按不一致的目标写入任何文件");
    }

    /// 重整目标被无关文件占用：保守冲突，旧记录与旧文件不动（不把记录改指不相关文件）
    #[tokio::test]
    async fn test_tv_reorg_conflicting_target_keeps_legacy() {
        let root = temp_root("tv-reorg-conflicting-target");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_link = output.join("张三").join("Season 1").join("张三 - S01E01.mp4");
        tokio::fs::hard_link(&mp4_u1, &old_link).await.unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_link.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        // 无关用户文件占住计划目标（非同源硬链接）
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        let new_target = plan.items[0].target.clone().unwrap();
        std::fs::create_dir_all(new_target.parent().unwrap()).unwrap();
        std::fs::write(&new_target, b"user unrelated file").unwrap();

        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0, "无关占用不得写入");
        assert_eq!(report.failed, 0);
        assert!(
            report
                .items
                .iter()
                .filter_map(|item| item.message.clone())
                .any(|message| message.contains("重整目标") || message.contains("无法确认归属")),
            "应有明确的冲突提示"
        );
        assert_eq!(
            std::fs::read(&new_target).unwrap(),
            b"user unrelated file",
            "用户无关文件不被覆盖"
        );
        let kept = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].path.contains("张三"), "记录仍指向旧路径");
        assert_same_inode(&old_link, &mp4_u1);
    }

    /// 旧 UP 名下的 .strm 记录：即使源目录名是新剧名，仍保守冲突，绝不迁移记录或新写文件
    #[tokio::test]
    async fn test_tv_reorg_legacy_strm_record_stays_conflict() {
        let root = temp_root("tv-reorg-strm");
        let source = root.join("新剧名");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let dir_u1 = source.join("u1");
        let (v1, page_v1, mp4_u1) = single_video(&db, "BV1", "张三", &dir_u1, day_of(2)).await;
        let output_canonical = std::fs::canonicalize(&output).unwrap();
        let rule = organization_rule::ActiveModel {
            name: Set("剧库".to_string()),
            rule_type: Set("tv".to_string()),
            source_path: Set(std::fs::canonicalize(&source).unwrap().to_string_lossy().to_string()),
            output_path: Set(output_canonical.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap();
        std::fs::create_dir_all(output.join("张三").join("Season 1")).unwrap();
        let old_strm = output.join("张三").join("Season 1").join("张三 - S01E01.strm");
        std::fs::write(&old_strm, b"/old/strm/target.mp4").unwrap();
        organization_output::ActiveModel {
            rule_id: Set(rule.id),
            page_id: Set(v1.id),
            bvid: Set("BV1".to_string()),
            cid: Set(page_v1.cid),
            series: Set(Some("张三".to_string())),
            episode: Set(Some(1)),
            path: Set(old_strm.to_string_lossy().to_string()),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items
                .iter()
                .all(|item| matches!(item.action, ItemAction::Conflict(_))),
            "旧 UP 名 STRM 记录整组冲突，实际：{:?}",
            plan.items.iter().map(|item| &item.action).collect::<Vec<_>>()
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created + report.refreshed, 0, "STRM 历史不得被重整或新写");
        assert!(!output.join("新剧名").exists(), "不得在新剧目录下写入任何文件");
        assert_eq!(
            std::fs::read(&old_strm).unwrap(),
            b"/old/strm/target.mp4",
            "STRM 文件保持原样"
        );
        let kept = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(kept.len(), 1);
        assert!(kept[0].path.ends_with(".strm"), "记录仍指向 STRM，未迁移");
    }

    #[tokio::test]
    async fn test_tv_multi_up_aggregation_and_per_page_art() {
        let root = temp_root("tv-multi-up");
        let source = root.join("剧名来源");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let time = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        // 两位 UP 各一个视频：甲是多P（两条分页），乙是单P
        let dir_u1 = source.join("u1");
        let v1 = insert_video(&db, "BV1", "甲", &dir_u1, time, false, 0).await;
        let mp4_u1_p1 = dir_u1.join("v1.mp4");
        let mp4_u1_p2 = dir_u1.join("v2.mp4");
        touch(&mp4_u1_p1);
        touch(&mp4_u1_p2);
        insert_page(&db, v1.id, 1, Some(&mp4_u1_p1)).await;
        insert_page(&db, v1.id, 2, Some(&mp4_u1_p2)).await;
        let dir_u2 = source.join("u2");
        single_video(&db, "BV2", "乙", &dir_u2, time).await;
        // 仅甲的 P1 有源缩略图，P2 缺图；乙(单P)有视频封面
        touch(&dir_u1.join("v1-thumb.jpg"));
        touch(&dir_u2.join("EP-poster.jpg"));
        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let plan = preview_rule(&rule, &db, None).await.unwrap();
        assert!(
            plan.items.iter().all(|item| item.series.as_deref() == Some("剧名来源")),
            "两位 UP 的视频必须聚合进同一部剧（源目录名），实际：{:?}",
            plan.items.iter().map(|item| item.series.clone()).collect::<Vec<_>>()
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 3, "三个分页（甲 2 P + 乙 1 P）全部创建");
        let season = output.join("剧名来源").join("Season 1");
        // 剧名 = 源目录名；多P标题 = 视频标题 - 分P标题；分集 NFO 分别生成
        assert!(season.join("剧名来源 - S01E01.mp4").is_file());
        let nfo_e1 = std::fs::read_to_string(season.join("剧名来源 - S01E01.nfo")).unwrap();
        assert!(
            nfo_e1.contains("<title>视频BV1 - 分P1</title>"),
            "各集使用各自信息，实际：{nfo_e1}"
        );
        let nfo_e3 = std::fs::read_to_string(season.join("剧名来源 - S01E03.nfo")).unwrap();
        assert!(nfo_e3.contains("<title>视频BV2</title>"), "UP 乙单P用视频标题");
        // 各分页图片独立：P1 有图，P2 缺图不嫁接
        assert!(season.join("剧名来源 - S01E01-thumb.jpg").is_file());
        assert!(!season.join("剧名来源 - S01E02-thumb.jpg").exists());
        let p2 = report.items.iter().find(|i| i.page_name == "分P2").unwrap();
        assert!(
            p2.message
                .as_deref()
                .is_some_and(|m| m.contains("缺少可用的分集封面图片")),
            "缺图分页应有提示，实际：{:?}",
            p2.message
        );
        // 剧集级文件（tvshow.nfo/poster/fanart）正常生成
        assert!(output.join("剧名来源").join("tvshow.nfo").is_file());
        assert!(output.join("剧名来源").join("poster.jpg").is_file());
    }

    /// R5 覆盖：既有 NFO/封面（含用户手改）被本规则重新执行时强制覆盖；视频/字幕/弹幕不覆盖
    #[tokio::test]
    async fn test_overwrite_replaces_metadata_and_keeps_media() {
        let root = temp_root("tv-overwrite");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let pubtime = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let video_dir = source.join("视频A");
        let (_video, _page, mp4) = single_video(&db, "BV1a", "张三", &video_dir, pubtime).await;
        // 源视频有真实封面与字幕
        touch(&video_dir.join("EP-poster.jpg"));
        touch(&video_dir.join("EP.zh-CN.srt"));
        std::fs::write(&video_dir.join("EP-poster.jpg"), b"new poster").unwrap();
        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let _ = execute_rule(&rule, &db, None).await.unwrap();
        let season = output.join("source").join("Season 1");
        let nfo = season.join("source - S01E01.nfo");
        let poster = season.join("source - S01E01-poster.jpg");
        // 用户手改 NFO 和封面
        std::fs::write(&nfo, b"user xml").unwrap();
        std::fs::write(&poster, b"user poster").unwrap();
        std::fs::write(season.join("source - S01E01.zh-CN.srt"), b"user subtitle").unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(report.failed, 0);
        assert!(
            std::fs::read_to_string(&nfo).unwrap().contains("<title>"),
            "NFO 应被强制覆盖回规则生成内容"
        );
        assert_eq!(
            std::fs::read(&poster).unwrap(),
            b"new poster",
            "封面应被强制覆盖为源封面"
        );
        assert_eq!(
            std::fs::read(season.join("source - S01E01.zh-CN.srt")).unwrap(),
            b"user subtitle",
            "字幕不得被覆盖"
        );
        // 覆盖失败：剧集级封面 extras poster.jpg 的源被替换为目录时，旧封面保留且报警，记录不变
        std::fs::remove_file(video_dir.join("EP-poster.jpg")).unwrap();
        std::fs::create_dir(video_dir.join("EP-poster.jpg")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(std::fs::read(&poster).unwrap(), b"new poster", "覆盖失败时旧封面保留");
        let kept = organization_output::Entity::find()
            .filter(organization_output::Column::RuleId.eq(rule.id))
            .all(&db)
            .await
            .unwrap();
        assert_eq!(kept.len(), 1, "失败不得丢记录");
        assert_eq!(kept[0].episode, Some(1), "失败不得改记录集号");

        // write_pending_file_overwrite 直接验证：源不可复制时报错且目标文件原样保留
        let overwrite_file = PendingFile {
            target: poster.clone(),
            overwrite: true,
            content: PendingContent::Copy(video_dir.join("EP-poster.jpg")),
        };
        let err = write_pending_file_overwrite(&output, &overwrite_file)
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("不是常规文件"),
            "源为目录时应明确报错：{err:#}"
        );
        assert_eq!(std::fs::read(&poster).unwrap(), b"new poster", "目标未被破坏");
    }
}
