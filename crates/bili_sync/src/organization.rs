//! 媒体整理：将已下载的视频以 STRM 形式组织为 Emby 媒体库目录（电视剧/电影）。
//! 原视频永远只读；所有写入都发生在规则的输出目录内，且绝不覆盖任何已存在的文件（只补缺）。
//! 执行全程持全局锁串行进行，持久记录先于文件写入，保证失败后无不可恢复孤儿。

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
fn path_occupied(path: &Path) -> bool {
    path.symlink_metadata().is_ok()
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
}

#[derive(Debug, Clone)]
pub enum PendingContent {
    /// 写入 STRM：内容为源视频文件的绝对路径
    Strm(PathBuf),
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
    /// STRM 目标路径
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

/// 分P路径解析符号链接后必须位于源目录内：防止 DB 路径被改动或符号链接让 STRM 指向源目录之外。
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

/// 目录归属判定：Ours = 本规则有持久整理记录（TV 按剧名、电影按 bvid 匹配）证明目录由本映射产出；
/// OtherOwned = 本规则其它剧集/视频的持久记录指向该目录；Foreign = 其它规则的记录位于该目录；
/// Unclaimed = 无任何记录，此时目录若存在则是用户目录或记录已丢失的另一作品，必须保守冲突。
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
    // 本规则已有的整理记录：稳定标识 -> 记录；历史记录保证集号持久、绝不复用
    let own_rows = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?;
    let own: HashMap<PageKey, &organization_output::Model> =
        own_rows.iter().map(|row| ((row.bvid.clone(), row.cid), row)).collect();
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
    for (video, page) in &missing_pages {
        let episode = own.get(&(video.bvid.clone(), page.cid)).and_then(|row| row.episode);
        plan.items.push(PlanItem {
            video_id: video.id,
            page_id: page.id,
            bvid: video.bvid.clone(),
            cid: page.cid,
            video_name: video.name.clone(),
            page_name: page.name.clone(),
            series: Some(video.upper_name.clone()),
            episode,
            action: ItemAction::Missing("源视频文件不存在".to_string()),
            target: None,
            files: Vec::new(),
            message: None,
        });
    }

    // 按UP用户名聚合为剧集，同名UP合并
    let mut series_map: HashMap<String, Vec<(video::Model, page::Model)>> = HashMap::new();
    for (video, pages) in candidates {
        for page in pages {
            series_map
                .entry(video.upper_name.clone())
                .or_default()
                .push((video.clone(), page));
        }
    }
    let mut series_names: Vec<_> = series_map.keys().cloned().collect();
    series_names.sort();
    // 目录名去重：不同剧名 filenamify 后同名时，先出现的剧名占用目录，后出现的整组按冲突跳过，绝不合并不同作品
    let mut used_series_stems: HashMap<String, String> = HashMap::new();
    for series in series_names {
        let entries = &series_map[&series];
        let computed_stem = filenamify(&series);
        // 同次内大小写别名：大小写不敏感文件系统上不同剧名仅大小写不同时是同一物理目录
        let stem_key = normalize_component(&computed_stem);
        if let Some(prev_series) = used_series_stems.get(&stem_key) {
            plan.warnings.push(format!(
                "剧集「{series}」与剧集「{prev_series}」的目录名「{computed_stem}」冲突，后者整组跳过，不合并"
            ));
            push_tv_conflict_group(
                &mut plan,
                entries,
                &series,
                format!("剧集目录名「{computed_stem}」已被剧集「{prev_series}」占用"),
            );
            continue;
        }
        // 跨次/既有目录归属检查：无论目录当前是否存在，都必须有本规则按该剧名的持久记录
        // 证明归属；否则保守冲突（记录可能先于写入，或目录事后被删除/移动）。
        let existing = find_existing_name(&output, &computed_stem)?;
        let series_stem = match &existing {
            ExistingName::One(name) => name.clone(),
            _ => computed_stem.clone(),
        };
        let series_dir = output.join(&series_stem);
        // 符号链接目录由执行期 ensure_path_within 报错，这里不重复判定归属
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
        match (&existing, &ownership) {
            (ExistingName::Ambiguous, _) => {
                plan.warnings.push(format!(
                    "输出目录下存在多个「{computed_stem}」的大小写变体，无法确认剧集「{series}」归属，整组跳过"
                ));
                push_tv_conflict_group(
                    &mut plan,
                    entries,
                    &series,
                    "存在多个大小写变体目录，无法确认归属".to_string(),
                );
                continue;
            }
            (_, DirOwnership::OtherOwned) => {
                plan.warnings.push(format!(
                    "剧集目录「{series_stem}」已有本规则其它剧集/视频的整理记录，剧集「{series}」整组跳过"
                ));
                push_tv_conflict_group(
                    &mut plan,
                    entries,
                    &series,
                    "目标目录已有本规则其它作品的整理记录，无法确认归属".to_string(),
                );
                continue;
            }
            (_, DirOwnership::Foreign(name)) => {
                plan.warnings.push(format!(
                    "剧集目录「{series_stem}」已被规则「{name}」的记录占用，剧集「{series}」整组跳过"
                ));
                push_tv_conflict_group(
                    &mut plan,
                    entries,
                    &series,
                    format!("剧集目录已被规则「{name}」的记录占用，无法确认归属"),
                );
                continue;
            }
            (_, DirOwnership::Unclaimed) if dir_exists => {
                plan.warnings.push(format!(
                    "剧集目录「{series_stem}」已存在但没有本规则（按剧名「{series}」）的整理记录，无法确认归属，整组跳过"
                ));
                push_tv_conflict_group(
                    &mut plan,
                    entries,
                    &series,
                    "目标目录已存在但无法确认归属（可能是用户目录或此前整理记录已丢失的另一作品）".to_string(),
                );
                continue;
            }
            _ => {}
        }
        used_series_stems.insert(stem_key, series.clone());
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
        // 已有记录的剧名与当前不一致（UP 改名）：目标已变化，按冲突处理，不改既有记录与文件，
        // 避免在新剧集下沿用旧集号与现有集数撞车或误覆盖既有目录中的文件
        let mut renamed_rows: HashMap<i32, &organization_output::Model> = HashMap::new();
        for (video, page) in entries {
            match own.get(&page_key(video, page)) {
                Some(row) if row.series.as_deref() != Some(series.as_str()) => {
                    renamed_rows.insert(page.id, row);
                }
                Some(row) => {
                    let episode = row.episode.unwrap_or(0);
                    max_episode = max_episode.max(episode);
                    numbered.push((page, video, episode));
                }
                None => new_pages.push((video.pubtime, video.id, page.pid, page, video)),
            }
        }
        new_pages.sort_by_key(|(pubtime, video_id, pid, _, _)| (*pubtime, *video_id, *pid));
        let mut episode_of: HashMap<i32, i32> = numbered.iter().map(|(page, _, episode)| (page.id, *episode)).collect();
        for (_, _, _, page, _) in &new_pages {
            max_episode += 1;
            episode_of.insert(page.id, max_episode);
        }
        // 集号重复检测（UP 改名、同名合并等极端情况下可能出现）
        let mut used: HashMap<i32, i32> = HashMap::new();
        for (page, _, episode) in &numbered {
            if let Some(other) = used.insert(*episode, page.id) {
                plan.warnings.push(format!(
                    "剧集「{series}」存在重复集号 {episode}（分页 {} 与 {}），请检查",
                    other, page.id
                ));
            }
        }
        // 剧集级附加文件：tvshow.nfo 必须生成（title 为 UP 用户名，即剧名），
        // 封面取投稿时间最早且已下载本地封面的代表视频
        let mut reps: Vec<&video::Model> = entries.iter().map(|(video, _)| video).collect();
        reps.sort_by_key(|video| video.pubtime);
        reps.dedup_by(|a, b| a.id == b.id);
        if let Some(rep) = reps.first() {
            plan.extras.push(PendingFile {
                target: series_dir.join("tvshow.nfo"),
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
                plan.extras.push(PendingFile {
                    target: series_dir.join("poster.jpg"),
                    content: PendingContent::Copy(poster.clone()),
                });
                plan.extras.push(PendingFile {
                    target: series_dir.join("fanart.jpg"),
                    content: PendingContent::Copy(poster),
                });
                break;
            }
        }
        for (video, page) in entries {
            if let Some(row) = renamed_rows.get(&page.id) {
                plan.items.push(PlanItem {
                    video_id: video.id,
                    page_id: page.id,
                    bvid: video.bvid.clone(),
                    cid: page.cid,
                    video_name: video.name.clone(),
                    page_name: page.name.clone(),
                    series: Some(series.clone()),
                    episode: row.episode,
                    action: ItemAction::Conflict(format!(
                        "该分页已记录在剧集「{}」下（UP 可能已改名），目标已变化，不修改既有记录与文件",
                        row.series.as_deref().unwrap_or("未知")
                    )),
                    target: Some(PathBuf::from(&row.path)),
                    files: Vec::new(),
                    message: None,
                });
                continue;
            }
            let episode = episode_of[&page.id];
            let target_stem = format!("{stem}{episode:0>2}");
            let mp4 = PathBuf::from(page.path.as_ref().expect("checked above"));
            let strm = season_dir.join(format!("{target_stem}.strm"));
            let action = if own.contains_key(&page_key(video, page)) {
                ItemAction::Refresh
            } else if let Some(other_rule_id) = other_keys.get(&page_key(video, page)) {
                ItemAction::Conflict(format!(
                    "该文件已被规则「{}」整理",
                    other_rules.get(other_rule_id).map(String::as_str).unwrap_or("未知")
                ))
            } else if path_occupied(&strm) {
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
                    target: strm.clone(),
                    content: PendingContent::Strm(mp4.clone()),
                },
                PendingFile {
                    target: season_dir.join(format!("{target_stem}.nfo")),
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
                action,
                target: Some(strm),
                files,
                message: None,
            });
        }
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
            let strm = folder.join(format!("{stem}.strm"));
            let action = if let Some(row) = own.get(&page_key(&video, page)) {
                // 记录的目标路径与当前计算结果不一致说明视频被重命名等，按冲突处理
                let computed = strm.to_string_lossy().to_string();
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
            } else if path_occupied(&strm) {
                ItemAction::Exists
            } else {
                ItemAction::Create
            };
            let mut files = vec![
                PendingFile {
                    target: strm.clone(),
                    content: PendingContent::Strm(mp4.clone()),
                },
                PendingFile {
                    target: folder.join(format!("{stem}.nfo")),
                    content: PendingContent::Xml(
                        NFO::Movie(Movie {
                            name: &video.name,
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
                action,
                target: Some(strm),
                files,
                message: None,
            });
        }
        if let Some(poster) = find_video_poster(
            &video.path,
            downloadable.first().and_then(|page| page.path.clone()).as_deref(),
        ) {
            plan.extras.push(PendingFile {
                target: folder.join("poster.jpg"),
                content: PendingContent::Copy(poster.clone()),
            });
            plan.extras.push(PendingFile {
                target: folder.join("fanart.jpg"),
                content: PendingContent::Copy(poster),
            });
        }
    }
    Ok(plan)
}

/// 执行计划：extras 与分页级文件一律只补缺，绝不覆盖任何已存在的输出（含既有用户字幕等）；
/// 持久记录先于文件写入——数据库失败则未写任何文件，文件写入失败则记录已在，刷新即可补齐恢复。
/// ponytail: ensure_path_within 前后两次检查之间存在理论 TOCTOU 窗口；如需绝对闭合需引入目录专有锁或 inotify 监控。
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
    // 本规则已有的整理记录：稳定标识 -> 记录 id
    let own: HashMap<PageKey, i32> = organization_output::Entity::find()
        .filter(organization_output::Column::RuleId.eq(rule.id))
        .all(db)
        .await?
        .into_iter()
        .map(|row| ((row.bvid, row.cid), row.id))
        .collect();
    for mut item in plan.items {
        match item.action.clone() {
            ItemAction::Create | ItemAction::Refresh => {
                let owned = own.contains_key(&(item.bvid.clone(), item.cid));
                // 本规则尚未记录的目标已被占用（非本规则文件）：一律跳过，绝不覆盖；
                // 已有记录（刷新）时目标存在属正常状态，由下方逐文件只补缺逻辑保证不覆盖
                if !owned && item.target.as_ref().is_some_and(|target| path_occupied(target)) {
                    item.action = ItemAction::Exists;
                    item.message = Some("目标文件已存在，未覆盖".to_string());
                    report.skipped += 1;
                    report.items.push(item);
                    continue;
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
                // 先落持久记录再写文件
                let record_result = if owned {
                    let row_id = own[&(item.bvid.clone(), item.cid)];
                    organization_output::ActiveModel {
                        id: Set(row_id),
                        series: Set(item.series.clone()),
                        episode: Set(item.episode),
                        path: Set(target_path),
                        ..Default::default()
                    }
                    .update(db)
                    .await
                    .map(|_| ())
                } else {
                    organization_output::ActiveModel {
                        rule_id: Set(rule.id),
                        page_id: Set(item.page_id),
                        bvid: Set(item.bvid.clone()),
                        cid: Set(item.cid),
                        series: Set(item.series.clone()),
                        episode: Set(item.episode),
                        path: Set(target_path),
                        ..Default::default()
                    }
                    .insert(db)
                    .await
                    .map(|_| ())
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
                for file in &item.files {
                    if path_occupied(&file.target) {
                        // 只补缺，不覆盖既有文件（含预先存在的用户字幕/弹幕/封面）
                        partial = true;
                        continue;
                    }
                    if let Err(e) = write_pending_file(output_root, file).await {
                        failure = Some(format!("写入「{}」失败：{:#}", file.target.display(), e));
                        break;
                    }
                }
                if let Some(message) = failure {
                    item.action = ItemAction::Failed(message);
                    item.message = item.action.action_message();
                    report.failed += 1;
                } else {
                    if partial {
                        item.message = Some("部分配套文件已存在，未覆盖".to_string());
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
    for extra in &plan.extras {
        let Some(dir) = extra.target.parent() else {
            continue;
        };
        if !report.items.iter().any(|item| {
            matches!(item.action, ItemAction::Create | ItemAction::Refresh)
                && item.target.as_ref().is_some_and(|target| target.starts_with(dir))
        }) || path_occupied(&extra.target)
        {
            continue;
        }
        match write_pending_file(output_root, extra).await {
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

/// 安全写入：先写唯一命名的临时文件（create_new 原子创建），再以硬链接原子落到目标位置——
/// hard_link 在目标已存在时失败，因此并发/竞态下也不会覆盖任何既有文件；临时文件随后删除。
/// ponytail: 不支持硬链接的文件系统（部分 NAS/网络盘/exFAT）会在此报错，无法整理该位置。
async fn write_pending_file(output_root: &Path, file: &PendingFile) -> Result<()> {
    ensure_path_within(output_root, &file.target)?;
    if let Some(parent) = file.target.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // 目录创建后复查：祖先路径中途被替换为符号链接时拒绝写入
    ensure_path_within(output_root, &file.target)?;
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
            PendingContent::Strm(source) => dst.write_all(source.to_string_lossy().as_bytes()).await?,
            PendingContent::Xml(xml) => dst.write_all(xml.as_bytes()).await?,
            PendingContent::Copy(source) => {
                let mut src = tokio::fs::File::open(source).await?;
                tokio::io::copy(&mut src, &mut dst).await?;
            }
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

/// 收集源视频旁的配套文件（字幕 srt、弹幕 ass、单集缩略图/封面），映射为输出目录中与 STRM 同名的文件。
/// STRM 播放时 Emby 只读取与其同目录、同主文件名的外挂字幕/弹幕，因此必须复制到输出侧。
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
                content: PendingContent::Copy(entry.path()),
            });
        }
    }
    // 单集图片：多页视频为 {stem}-thumb.jpg，单页视频为 {stem}-poster.jpg
    for suffix in ["-thumb.jpg", "-poster.jpg"] {
        let source = dir.join(format!("{src_stem}{suffix}"));
        if source.is_file() {
            files.push(PendingFile {
                target: target_dir.join(format!("{target_stem}{suffix}")),
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

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bili-sync-org-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

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
        let _ = std::fs::remove_dir_all(root);
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
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn test_tv_numbering_history_and_appending() {
        let root = temp_root("tv");
        let source = root.join("source");
        let output = root.join("output");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&output).unwrap();
        let db = test_db(&root).await;
        let day = |d: u32| {
            chrono::NaiveDate::from_ymd_opt(2024, 1, d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        };

        // 视频 A：1月2日投稿，单P；视频 B：1月1日投稿，3P。同一UP「张三」
        let video_a_dir = source.join("视频A");
        let (video_a, page_a, mp4_a) = single_video(&db, "BV1a", "张三", &video_a_dir, day(2)).await;
        // 为 A 的分页准备封面与弹幕，检验配套文件复制
        touch(&video_a_dir.join("EP-poster.jpg"));
        touch(&video_a_dir.join("EP.zh-CN.default.ass"));
        touch(&video_a_dir.join("EP.zh-CN.srt"));

        let video_b_dir = source.join("视频B").join("Season 1");
        let video_b = insert_video(&db, "BV1b", "张三", video_b_dir.parent().unwrap(), day(1), false, 0).await;
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
        let season = output.join("张三").join("Season 1");
        let strm = season.join("张三 - S01E04.strm");
        assert_eq!(
            std::fs::read_to_string(&strm).unwrap(),
            mp4_a.to_string_lossy(),
            "STRM 内容为源视频绝对路径"
        );
        // 单P集标题 = 视频标题；多P集标题 = 视频标题 - 分P标题；tvshow.nfo 标题 = UP 用户名
        let nfo_a = std::fs::read_to_string(season.join("张三 - S01E04.nfo")).unwrap();
        assert!(nfo_a.contains("<title>视频BV1a</title>"), "单P集标题应为视频标题");
        let nfo_b1 = std::fs::read_to_string(season.join("张三 - S01E01.nfo")).unwrap();
        assert!(
            nfo_b1.contains("<title>视频BV1b - 分P1</title>"),
            "多P集标题应为 视频标题 - 分P标题"
        );
        let tvshow = std::fs::read_to_string(output.join("张三").join("tvshow.nfo")).unwrap();
        assert!(tvshow.contains("<title>张三</title>"), "tvshow.nfo 标题应为 UP 用户名");
        assert!(
            season.join("张三 - S01E04.zh-CN.default.ass").is_file(),
            "弹幕复制到输出侧"
        );
        assert!(season.join("张三 - S01E04.zh-CN.srt").is_file());
        assert!(season.join("张三 - S01E04-poster.jpg").is_file());
        assert!(output.join("张三").join("poster.jpg").is_file());

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
        let (video_c, _, _) = single_video(&db, "BV1c", "张三", &video_c_dir, day(3)).await;
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
        let (video_d, _, _) = single_video(&db, "BV1d", "张三", &video_d_dir, day(4)).await;
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
        let _ = std::fs::remove_dir_all(root);
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
        let (_video, _page, _mp4) = single_video(&db, "BV1a", "张三", &video_dir, pubtime).await;
        touch(&video_dir.join("EP.zh-CN.srt"));

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);
        let season = output.join("张三").join("Season 1");
        let strm = season.join("张三 - S01E01.strm");
        let srt = season.join("张三 - S01E01.zh-CN.srt");

        // 用户修改本规则生成的字幕：刷新后保持用户内容不变，只补缺缺失文件
        std::fs::write(&srt, b"user subtitle").unwrap();
        std::fs::remove_file(season.join("张三 - S01E01.nfo")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(
            std::fs::read(&srt).unwrap(),
            b"user subtitle",
            "刷新不得覆盖已存在的输出文件（含用户修改过的字幕）"
        );
        assert!(season.join("张三 - S01E01.nfo").is_file(), "缺失文件被补齐");
        assert!(report.items[0].message.as_deref().is_some_and(|m| m.contains("未覆盖")));

        // 用户替换了 STRM 本体：刷新跳过该文件（不覆盖），其余文件补缺，记录更新
        std::fs::write(&strm, b"user strm").unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.refreshed, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(std::fs::read(&strm).unwrap(), b"user strm", "不得覆盖用户替换的 STRM");
        assert!(report.items[0].message.as_deref().is_some_and(|m| m.contains("未覆盖")));
        let _ = std::fs::remove_dir_all(root);
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
        assert_eq!(versions[0], "视频BV1m2 (2019) - P01.strm", "多P版本名由固有 pid 决定");
        assert_eq!(versions[1], "视频BV1m2 (2019) - P02.strm");
        assert_eq!(versions[2], "视频BV1m2 (2019) - P06.strm");

        // 执行必须显式选择
        assert!(
            execute_rule(&rule, &db, None).await.is_err(),
            "电影执行缺少 video_ids 应报错"
        );
        let report = execute_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        assert_eq!(report.created, 1);
        let folder_m1 = output.join("视频BV1m1 (2019)");
        assert_eq!(
            std::fs::read_to_string(folder_m1.join("视频BV1m1 (2019).strm")).unwrap(),
            mp4_m1.to_string_lossy()
        );
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
                "视频BV1m2 (2019) - P02.strm".to_string(),
                "视频BV1m2 (2019) - P06.strm".to_string(),
            ],
            "首P缺失不得改变其它版本的命名"
        );
        touch(&video_m2_dir.join("v1.mp4"));
        let report = execute_rule(&rule, &db, Some(&[video_m2.id])).await.unwrap();
        assert_eq!(report.created, 3, "三个版本全部创建");
        let folder_m2 = output.join("视频BV1m2 (2019)");
        assert!(folder_m2.join("视频BV1m2 (2019) - P01.strm").is_file());
        assert!(folder_m2.join("视频BV1m2 (2019) - P06.strm").is_file());

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
        active.path = Set("/somewhere/else.strm".to_string());
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
        std::fs::write(folder_m1.join("视频BV1m1 (2019).strm"), b"user file").unwrap();
        let report = execute_rule(&rule, &db, Some(&[video_m1.id])).await.unwrap();
        assert_eq!(report.skipped, 1);
        assert_eq!(report.created, 0);
        assert_eq!(
            std::fs::read(folder_m1.join("视频BV1m1 (2019).strm")).unwrap(),
            b"user file",
            "不覆盖非规则文件"
        );
        let _ = std::fs::remove_dir_all(root);
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
        let (_, _, _) = single_video(&db, "BV1a", "张三", &video_dir, pubtime).await;

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
        let season = output.join("张三").join("Season 1");
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
        assert!(season.join("张三 - S01E01.strm").is_file());
        let _ = std::fs::remove_dir_all(root);
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
        let (_, _, mp4) = single_video(&db, "BV1a", "张三", &video_dir, pubtime).await;

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
        assert_eq!(
            std::fs::read_to_string(output.join("张三").join("Season 1").join("张三 - S01E01.strm")).unwrap(),
            mp4.to_string_lossy()
        );
        let _ = std::fs::remove_dir_all(root);
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
        let (_, _, _) = single_video(&db, "BV1a", "张三", &video_dir, pubtime).await;

        let rule = rule_model(
            "剧库",
            "tv",
            &std::fs::canonicalize(&source).unwrap(),
            &std::fs::canonicalize(&output).unwrap(),
        );
        // 剧集目录本身是指向输出目录之外的符号链接
        #[cfg(unix)]
        std::os::unix::fs::symlink(&external, output.join("张三")).unwrap();
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
        std::fs::remove_file(output.join("张三")).unwrap();
        let report = execute_rule(&rule, &db, None).await.unwrap();
        assert_eq!(report.created, 1);
        assert!(
            output
                .join("张三")
                .join("Season 1")
                .join("张三 - S01E01.strm")
                .is_file()
        );
        let _ = std::fs::remove_dir_all(root);
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
        let user_dir = output.join("UP");
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
            series: Set(Some("UP".to_string())),
            episode: Set(Some(1)),
            path: Set(user_dir
                .join("Season 1")
                .join("UP - S01E01.strm")
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
            user_dir.join("Season 1").join("UP - S01E01.strm").is_file(),
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
        let a_strm = shared_dir.join("同名电影 (2024).strm");
        let a_content = std::fs::read(&a_strm).unwrap();
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
        assert_eq!(std::fs::read(&a_strm).unwrap(), a_content, "既有作品的文件不得被改动");
        assert!(
            !shared_dir.join("同名电影 (2024) - P01.strm").exists(),
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
        let _ = std::fs::remove_dir_all(root);
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
        let gone_dir = output.join("UP甲");
        organization_output::ActiveModel {
            rule_id: Set(rule_a.id),
            page_id: Set(1),
            bvid: Set("BVold".to_string()),
            cid: Set(1),
            series: Set(Some("UP甲".to_string())),
            episode: Set(Some(1)),
            path: Set(gone_dir
                .join("Season 1")
                .join("UP甲 - S01E01.strm")
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
            series: Set(Some("UP/乙".to_string())),
            episode: Set(Some(1)),
            path: Set(output
                .join("UP_乙")
                .join("Season 1")
                .join("x.strm")
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
        let _ = std::fs::remove_dir_all(root);
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

        // 同目录「UP」下：一条属于剧名 UP 的记录（匹配）+ 一条属于剧名 OTHER 的记录（不匹配）
        let up_dir = output.join("UP");
        for (page_id, series, bvid) in [(1, "UP", "BV1"), (2, "OTHER", "BV2")] {
            organization_output::ActiveModel {
                rule_id: Set(rule.id),
                page_id: Set(page_id),
                bvid: Set(bvid.to_string()),
                cid: Set(page_id as i64),
                series: Set(Some(series.to_string())),
                episode: Set(Some(1)),
                path: Set(up_dir
                    .join("Season 1")
                    .join(format!("{series} - S01E01.strm"))
                    .to_string_lossy()
                    .to_string()),
                ..Default::default()
            }
            .insert(&db)
            .await
            .unwrap();
        }
        single_video(&db, "BV1", "UP", &source.join("v1"), time).await;
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
            series: Set(Some("UP".to_string())),
            episode: Set(Some(5)),
            path: Set(up_dir.join("Season 1").join("x.strm").to_string_lossy().to_string()),
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

        // 目录不存在时的大小写别名：记录路径为 .../UP/...，待判定目录为 .../up（目录均不存在）
        let up_lower = output.join("up");
        assert!(!up_lower.exists());
        assert!(
            stored_path_under(&up_dir.join("Season 1").join("a.strm").to_string_lossy(), &up_lower),
            "大小写别名在目录不存在时应按路径键识别"
        );
        // 反向也成立
        assert!(stored_path_under(&up_lower.join("a.strm").to_string_lossy(), &up_dir));
        // 非前缀路径不得误判
        assert!(!stored_path_under(
            &output.join("UPX").join("a.strm").to_string_lossy(),
            &up_lower
        ));
        let _ = std::fs::remove_dir_all(root);
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
            series: Set(Some("张三".to_string())),
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
        let _ = std::fs::remove_dir_all(root);
    }
}
