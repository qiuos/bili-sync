//! 批量清理候选记录的选取与执行：
//! 1. B 站失效记录（valid = false）；
//! 2. 已下载完成且所有分 P 均确实成功下载、而其视频文件均已确认不存在的记录。
//!
//! 清理只删除数据库记录，绝不删除本地文件。选取时对本地状态做保守校验，
//! 部分文件缺失、分页下载失败（含达到最大重试）、源目录不可访问或状态存疑的记录
//! 一律不按本地缺失清理。执行只处理调用方确认过的候选 ID，并在执行时重新核验。

use std::collections::HashMap;
use std::io::ErrorKind;

use bili_sync_entity::*;
use sea_orm::ActiveValue::{Set, Unchanged};
use sea_orm::sea_query::Expr;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, DerivePartialModel, EntityTrait,
    FromQueryResult, QueryFilter, QuerySelect, TransactionTrait,
};

/// video.download_status 完成标记位（utils::status::STATUS_COMPLETED = 1 << 31）的 SQL 判定
const SQL_DOWNLOAD_COMPLETED: &str = "(video.download_status & 2147483648) != 0";

/// 分页「视频内容」子任务（PageStatus offset 1）下载成功（STATUS_OK = 0b111）的 SQL 判定；
/// 达到最大重试的失败状态（0b100）不视为成功，未成功下载过的分页不纳入本地缺失判定
const SQL_PAGE_DOWNLOADED: &str = "((page.download_status >> 3) & 7) = 7";

/// 每批 SQL 绑定变量数量上限，与既有批量操作保持一致
const BATCH_SIZE: usize = 500;

/// 清理候选：invalid 为 B 站失效记录，missing 为已下载完成且全部分 P 文件均不存在的记录（两者互斥）。
/// skipped_source_paths 为无法确认磁盘可用的来源目录（详见 find_missing_video_ids），其下候选全部保留
pub struct CleanupSelection {
    pub invalid_ids: Vec<i32>,
    pub missing_ids: Vec<i32>,
    pub skipped_source_paths: Vec<String>,
}

/// 清理执行结果：删除的视频与分页记录数量，以及重新核验后不再匹配而被跳过的候选数量
pub struct CleanupReport {
    pub deleted_videos_count: u64,
    pub deleted_pages_count: u64,
    pub skipped_count: usize,
}

#[derive(DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "video::Entity")]
struct CleanupCandidate {
    id: i32,
    #[allow(dead_code)]
    path: String,
    collection_id: Option<i32>,
    favorite_id: Option<i32>,
    submission_id: Option<i32>,
    watch_later_id: Option<i32>,
}

/// 预览可清理的候选。执行时应把返回的 ID 集合作为确认结果传给 execute_cleanup_records
pub async fn select_cleanup_targets(db: &DatabaseConnection) -> Result<CleanupSelection, sea_orm::DbErr> {
    let invalid_ids = find_invalid_video_ids(db, None).await?;
    let (missing_ids, skipped_source_paths) = find_missing_video_ids(db, None).await?;
    Ok(CleanupSelection {
        invalid_ids,
        missing_ids,
        skipped_source_paths,
    })
}

/// 执行清理：只处理调用方确认过的候选 ID，并重新核验与预览一致的判定条件，
/// 预览到执行之间状态变化的候选会被跳过而不会删除。本地文件一律保留；
/// 文件缺失记录删除后回退来源扫描水位，保证重新扫描能重新发现并重新下载
pub async fn execute_cleanup_records(
    db: &DatabaseConnection,
    invalid_ids: &[i32],
    missing_ids: &[i32],
) -> Result<CleanupReport, sea_orm::DbErr> {
    let invalid_verified = find_invalid_video_ids(db, Some(invalid_ids)).await?;
    let (missing_verified, _) = find_missing_video_ids(db, Some(missing_ids)).await?;
    let mut report = CleanupReport {
        deleted_videos_count: 0,
        deleted_pages_count: 0,
        skipped_count: (invalid_ids.len() - invalid_verified.len()) + (missing_ids.len() - missing_verified.len()),
    };
    let ids: Vec<i32> = invalid_verified
        .iter()
        .copied()
        .chain(missing_verified.iter().copied())
        .collect();
    if ids.is_empty() {
        return Ok(report);
    }
    let txn = db.begin().await?;
    // 删除前取出文件缺失记录，删除后据其回退来源水位；B 站失效记录不回退，避免无意义地复活失效记录
    let missing_models = video::Entity::find()
        .filter(video::Column::Id.is_in(missing_verified.iter().copied()))
        .all(&txn)
        .await?;
    for chunk in ids.chunks(BATCH_SIZE) {
        let result = page::Entity::delete_many()
            .filter(page::Column::VideoId.is_in(chunk.iter().copied()))
            .exec(&txn)
            .await?;
        report.deleted_pages_count += result.rows_affected;
    }
    for chunk in ids.chunks(BATCH_SIZE) {
        let result = video::Entity::delete_many()
            .filter(video::Column::Id.is_in(chunk.iter().copied()))
            .exec(&txn)
            .await?;
        report.deleted_videos_count += result.rows_affected;
    }
    for model in missing_models {
        rollback_source_latest_row_at(&txn, &model).await?;
    }
    txn.commit().await?;
    Ok(report)
}

/// B 站失效记录：详情接口 -404 后被标记 valid = false 的视频。
/// restrict 传入时只核验给定 ID（供执行阶段对确认过的候选重新核验）
async fn find_invalid_video_ids(db: &DatabaseConnection, restrict: Option<&[i32]>) -> Result<Vec<i32>, sea_orm::DbErr> {
    let mut query = video::Entity::find().filter(video::Column::Valid.eq(false));
    if let Some(ids) = restrict {
        query = query.filter(video::Column::Id.is_in(ids.iter().copied()));
    }
    query
        .select_only()
        .column(video::Column::Id)
        .into_tuple::<i32>()
        .all(db)
        .await
}

/// 已下载完成且所有分 P 文件均确认不存在的记录。以下情况一律不视为本地缺失，不做清理：
/// 未下载完成、分页未成功下载（含达到最大重试的失败分页）、分页路径缺失、
/// 来源记录或源目录不可访问、视频目录状态存疑（非 NotFound 的错误）、任一文件仍存在、
/// 权限或 I/O 错误导致无法确认文件状态，以及同源证据不足（见下）。
///
/// 同源级别的保守校验：源目录本身往往是挂载点，卸载后留下的空目录 stat 依然成功，
/// 无法凭 stat 区分「已挂载的空磁盘」与「卸载后的残留空目录」。因此要求该源范围内
/// 至少有一个已下载文件规范化后严格位于源目录内且为常规文件（stat 成功），
/// 否则该源的全部缺失候选整源跳过（宁可保留，也不能因磁盘未挂载误删）。
/// 注意：这只是无法证伪时的保守跳过判据，不构成「磁盘已挂载」的证明——
/// 源目录内某个子路径可访问不能证明其他子挂载点存在或可用。
/// restrict 传入时只在最终结果上限定给定 ID，候选判定与同源证据始终在全部候选上计算，
/// 保证执行阶段与预览阶段核验一致；返回值同时给出被跳过的来源目录，供预览接口提示用户
async fn find_missing_video_ids(
    db: &DatabaseConnection,
    restrict: Option<&[i32]>,
) -> Result<(Vec<i32>, Vec<String>), sea_orm::DbErr> {
    let query = video::Entity::find()
        .filter(video::Column::Valid.eq(true))
        .filter(Expr::cust(SQL_DOWNLOAD_COMPLETED))
        // 至少一页确实成功下载且记录了路径
        .filter(Expr::cust(format!(
            "EXISTS (SELECT 1 FROM page WHERE page.video_id = video.id AND {SQL_PAGE_DOWNLOADED} AND page.path IS NOT NULL AND page.path != '')"
        )))
        // 不存在任何未记录路径或未成功下载的分页，失败耗尽的分页不能按本地缺失解释
        .filter(Expr::cust(format!(
            "NOT EXISTS (SELECT 1 FROM page WHERE page.video_id = video.id AND (page.path IS NULL OR page.path = '' OR NOT ({SQL_PAGE_DOWNLOADED})))"
        )));
    let candidates = query
        .select_only()
        .columns([
            video::Column::Id,
            video::Column::Path,
            video::Column::CollectionId,
            video::Column::FavoriteId,
            video::Column::SubmissionId,
            video::Column::WatchLaterId,
        ])
        .into_model::<CleanupCandidate>()
        .all(db)
        .await?;
    if candidates.is_empty() {
        return Ok((vec![], vec![]));
    }
    let sources = load_source_paths(db).await?;
    let candidate_ids = candidates.iter().map(|c| c.id).collect::<Vec<_>>();
    let mut page_paths: HashMap<i32, Vec<String>> = HashMap::new();
    for chunk in candidate_ids.chunks(BATCH_SIZE) {
        let rows: Vec<(i32, Option<String>)> = page::Entity::find()
            .filter(page::Column::VideoId.is_in(chunk.iter().copied()))
            .select_only()
            .columns([page::Column::VideoId, page::Column::Path])
            .into_tuple()
            .all(db)
            .await?;
        for (video_id, path) in rows {
            page_paths.entry(video_id).or_default().push(path.unwrap_or_default());
        }
    }

    // 逐条候选判定文件缺失；同时按源目录收集全部候选记录的文件路径留作同源证据，
    // 供后续「文件确实位于该源的文件系统上」校验使用（restrict 不影响判定口径，确保与预览一致）
    let mut missing_ids = Vec::new();
    let mut candidate_source: HashMap<i32, String> = HashMap::new();
    let mut evidence_paths: HashMap<String, Vec<String>> = HashMap::new();
    let mut source_accessible: HashMap<String, bool> = HashMap::new();
    for candidate in &candidates {
        let Some(source_path) = source_path_of(candidate, &sources) else {
            // 来源记录不存在（可能已删除订阅），无法核验目录状态，保守跳过
            continue;
        };
        let accessible = match source_accessible.get(source_path) {
            Some(&a) => a,
            None => {
                let a = is_accessible_dir(source_path).await;
                source_accessible.insert(source_path.clone(), a);
                a
            }
        };
        candidate_source.insert(candidate.id, source_path.clone());
        evidence_paths
            .entry(source_path.clone())
            .or_default()
            .extend(page_paths.get(&candidate.id).cloned().unwrap_or_default());
        if accessible && all_files_confirmed_missing(page_paths.get(&candidate.id).map_or(&[][..], Vec::as_slice)).await
        {
            missing_ids.push(candidate.id);
        }
    }

    // 同源证据校验：该源下没有任何一条「规范化后严格位于源目录内且为常规文件」的已下载文件
    // stat 成功时，视为无法确认磁盘可用，整源剔除候选。注意 stat 成功不等于磁盘可用
    //（卸载后的空挂载点目录依旧 stat 成功），文件位于源目录内也只说明该文件所在子路径可用，
    // 不能证明源下其他子挂载点、也未证明磁盘已挂载；此处只是保守的跳过判据而非证明
    let mut missing_by_source: HashMap<String, Vec<i32>> = HashMap::new();
    for id in &missing_ids {
        if let Some(source_path) = candidate_source.get(id) {
            missing_by_source.entry(source_path.clone()).or_default().push(*id);
        }
    }
    let mut skipped_source_paths = Vec::new();
    for (source_path, ids) in missing_by_source {
        let mut has_evidence = false;
        if let Some(paths) = evidence_paths.get(&source_path) {
            for path in paths {
                if is_inside_source_and_regular_file(path, &source_path).await {
                    has_evidence = true;
                    break;
                }
            }
        }
        if has_evidence {
            continue;
        }
        missing_ids.retain(|id| !ids.contains(id));
        skipped_source_paths.push(source_path);
    }
    skipped_source_paths.sort();

    if let Some(ids) = restrict {
        missing_ids.retain(|id| ids.contains(id));
    }
    Ok((missing_ids, skipped_source_paths))
}

/// 视频所属视频源的目录路径；每条视频只会属于一个来源
fn source_path_of<'a>(candidate: &CleanupCandidate, sources: &'a SourcePaths) -> Option<&'a String> {
    if let Some(id) = candidate.favorite_id {
        sources.favorite.get(&id)
    } else if let Some(id) = candidate.collection_id {
        sources.collection.get(&id)
    } else if let Some(id) = candidate.submission_id {
        sources.submission.get(&id)
    } else {
        candidate.watch_later_id.and_then(|id| sources.watch_later.get(&id))
    }
}

#[derive(Default)]
struct SourcePaths {
    favorite: HashMap<i32, String>,
    collection: HashMap<i32, String>,
    submission: HashMap<i32, String>,
    watch_later: HashMap<i32, String>,
}

async fn load_source_paths(db: &DatabaseConnection) -> Result<SourcePaths, sea_orm::DbErr> {
    let (favorites, collections, submissions, watch_laters) = tokio::try_join!(
        favorite::Entity::find()
            .select_only()
            .columns([favorite::Column::Id, favorite::Column::Path])
            .into_tuple::<(i32, String)>()
            .all(db),
        collection::Entity::find()
            .select_only()
            .columns([collection::Column::Id, collection::Column::Path])
            .into_tuple::<(i32, String)>()
            .all(db),
        submission::Entity::find()
            .select_only()
            .columns([submission::Column::Id, submission::Column::Path])
            .into_tuple::<(i32, String)>()
            .all(db),
        watch_later::Entity::find()
            .select_only()
            .columns([watch_later::Column::Id, watch_later::Column::Path])
            .into_tuple::<(i32, String)>()
            .all(db),
    )?;
    let to_map = |rows: Vec<(i32, String)>| rows.into_iter().collect::<HashMap<_, _>>();
    Ok(SourcePaths {
        favorite: to_map(favorites),
        collection: to_map(collections),
        submission: to_map(submissions),
        watch_later: to_map(watch_laters),
    })
}

/// 目录是否存在且可访问（stat 成功且为目录）
async fn is_accessible_dir(path: &str) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|meta| meta.is_dir())
        .unwrap_or(false)
}

/// 判定路径是否可作为该源目录的挂载证据：规范化后必须严格位于规范化源目录内且是常规文件。
/// 即使通过这些检查也只说明「该文件确实在源目录所在文件系统上」，不能证明其它子挂载点存在，
/// 因此调用方只能以此作为保守的跳过判据，而不是对磁盘已挂载的证明
async fn is_inside_source_and_regular_file(path: &str, source: &str) -> bool {
    let Ok(path) = std::path::Path::new(path).canonicalize() else {
        return false;
    };
    let Ok(source) = dunce::canonicalize(std::path::Path::new(source)) else {
        return false;
    };
    path.starts_with(&source) && tokio::fs::metadata(&path).await.is_ok_and(|m| m.is_file())
}

/// 所有文件均已确认不存在：仅 stat 返回 NotFound 才算缺失，其余错误（权限、I/O 等）视为无法判定
async fn all_files_confirmed_missing(paths: &[String]) -> bool {
    for path in paths {
        match tokio::fs::metadata(path).await {
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            _ => return false,
        }
    }
    true
}

/// 各来源用于比较的时间不同：收藏夹/稍后再看取 favtime，合集取 pubtime，投稿取 ctime
/// （与 adapter 的 release_datetime 及水位迁移脚本保持一致）
pub(super) async fn rollback_source_latest_row_at(
    txn: &DatabaseTransaction,
    video: &video::Model,
) -> Result<(), sea_orm::DbErr> {
    let rollback_at = if video.favorite_id.is_some() || video.watch_later_id.is_some() {
        video.favtime
    } else if video.collection_id.is_some() {
        video.pubtime
    } else if video.submission_id.is_some() {
        video.ctime
    } else {
        return Ok(());
    };
    // 扫描以 release_datetime > latest_row_at 判定新视频，回退一秒确保被删除的视频仍在扫描窗口内
    let rollback_at = rollback_at - chrono::Duration::seconds(1);
    if let Some(id) = video.favorite_id
        && let Some(source) = favorite::Entity::find_by_id(id).one(txn).await?
        && source.latest_row_at > rollback_at
    {
        favorite::ActiveModel {
            id: Unchanged(id),
            latest_row_at: Set(rollback_at),
            ..Default::default()
        }
        .update(txn)
        .await?;
    }
    if let Some(id) = video.collection_id
        && let Some(source) = collection::Entity::find_by_id(id).one(txn).await?
        && source.latest_row_at > rollback_at
    {
        collection::ActiveModel {
            id: Unchanged(id),
            latest_row_at: Set(rollback_at),
            ..Default::default()
        }
        .update(txn)
        .await?;
    }
    if let Some(id) = video.submission_id
        && let Some(source) = submission::Entity::find_by_id(id).one(txn).await?
        && source.latest_row_at > rollback_at
    {
        submission::ActiveModel {
            id: Unchanged(id),
            latest_row_at: Set(rollback_at),
            ..Default::default()
        }
        .update(txn)
        .await?;
    }
    if let Some(id) = video.watch_later_id
        && let Some(source) = watch_later::Entity::find_by_id(id).one(txn).await?
        && source.latest_row_at > rollback_at
    {
        watch_later::ActiveModel {
            id: Unchanged(id),
            latest_row_at: Set(rollback_at),
            ..Default::default()
        }
        .update(txn)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bili_sync_migration::{Migrator, MigratorTrait};
    use sea_orm::{ActiveModelTrait, ActiveValue::Set, TryIntoModel};

    use super::*;
    use crate::utils::status::STATUS_COMPLETED;

    /// 测试临时根：优先取环境变量 BILI_SYNC_ORG_TEST_ROOT（指向已登记的任务临时根），缺省为系统临时目录；
    /// 在其下用 UUID 创建唯一、不可预测的子目录。测试绝不删除任何目录，
    /// 统一由登记的清理流程或系统临时目录策略回收。
    fn temp_root(name: &str) -> std::path::PathBuf {
        let base = std::env::var("BILI_SYNC_ORG_TEST_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir());
        let dir = base.join(format!("{name}-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&dir).unwrap();
        dir
    }

    async fn test_db(dir: &std::path::Path) -> DatabaseConnection {
        let db = sea_orm::Database::connect(format!("sqlite://{}?mode=rwc", dir.join("data.sqlite").display()))
            .await
            .unwrap();
        Migrator::up(&db, None).await.unwrap();
        db
    }

    async fn insert_source(db: &DatabaseConnection, path: &std::path::Path) -> favorite::Model {
        favorite::ActiveModel {
            f_id: Set(1),
            name: Set("测试收藏夹".to_string()),
            path: Set(path.to_string_lossy().to_string()),
            enabled: Set(true),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_video(
        db: &DatabaseConnection,
        favorite_id: i32,
        bvid: &str,
        path: &std::path::Path,
        valid: bool,
        completed: bool,
    ) -> video::Model {
        video::ActiveModel {
            bvid: Set(bvid.to_string()),
            favorite_id: Set(Some(favorite_id)),
            upper_id: Set(1),
            upper_name: Set("UP主".to_string()),
            upper_face: Set(String::new()),
            name: Set(format!("视频{bvid}")),
            path: Set(path.to_string_lossy().to_string()),
            category: Set(2),
            intro: Set(String::new()),
            cover: Set(String::new()),
            ctime: Set(chrono::NaiveDateTime::default()),
            pubtime: Set(chrono::NaiveDateTime::default()),
            favtime: Set(chrono::NaiveDateTime::default()),
            download_status: Set(if completed { STATUS_COMPLETED } else { 0 }),
            valid: Set(valid),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    /// 构造分页 download_status：offset 1 是「视频内容」子任务，0b111 = 成功，0b100 = 失败耗尽
    fn page_status(video_content: u32) -> u32 {
        video_content << 3
    }

    async fn insert_page(
        db: &DatabaseConnection,
        video_id: i32,
        pid: i32,
        mp4: Option<&std::path::Path>,
        download_status: u32,
    ) -> page::Model {
        page::ActiveModel {
            video_id: Set(video_id),
            cid: Set(pid as i64),
            pid: Set(pid),
            name: Set(format!("分P{pid}")),
            duration: Set(0),
            path: Set(mp4.map(|path| path.to_string_lossy().to_string())),
            download_status: Set(download_status),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    async fn select(db: &DatabaseConnection) -> (Vec<i32>, Vec<i32>) {
        let selection = select_cleanup_targets(db).await.unwrap();
        (selection.invalid_ids, selection.missing_ids)
    }

    /// 模拟未挂载的挂载点：卸载后残留的空目录 stat 依旧成功，
    /// 但该源下没有任何可确认存在的文件，缺失候选必须整源跳过并给出提示
    #[tokio::test]
    async fn unmounted_mount_point_skips_whole_source() {
        let dir = temp_root("cleanup-unmounted");
        let source_dir = dir.join("source");
        // 空的源目录：模拟卸载后残留的挂载点，stat 成功但没有任何磁盘证据
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        let video = insert_video(&db, source.id, "BV1unmounted", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        let selection = select_cleanup_targets(&db).await.unwrap();
        assert!(selection.missing_ids.is_empty());
        assert_eq!(
            selection.skipped_source_paths,
            vec![source_dir.to_string_lossy().to_string()]
        );

        // 执行阶段同样拒绝：即使调用方确认该候选也不得删除
        let report = execute_cleanup_records(&db, &[], &[video.id]).await.unwrap();
        assert_eq!(report.deleted_videos_count, 0);
        assert_eq!(report.skipped_count, 1);
        assert!(video::Entity::find_by_id(video.id).one(&db).await.unwrap().is_some());
    }

    /// 同源中只要有一个视频文件确认存在，其它确认缺失的视频仍可正常成为候选
    #[tokio::test]
    async fn one_existing_file_evidence_keeps_other_candidates() {
        let dir = temp_root("cleanup-evidence");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        // 仍存在的视频：既是「磁盘已挂载」的证据，也证明部分文件存在不会整源跳过
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1evidence", &keep_dir, true, true).await;
        touch(&keep_dir.join("0.mp4"));
        insert_page(&db, keep_video.id, 0, Some(&keep_dir.join("0.mp4")), page_status(7)).await;
        // 整目录被删除的视频：确认缺失，应进入候选
        let gone_dir = source_dir.join("gone");
        std::fs::create_dir_all(&gone_dir).unwrap();
        let gone_video = insert_video(&db, source.id, "BV1gone", &gone_dir, true, true).await;
        insert_page(&db, gone_video.id, 0, Some(&gone_dir.join("0.mp4")), page_status(7)).await;
        std::fs::remove_dir_all(&gone_dir).unwrap();

        let selection = select_cleanup_targets(&db).await.unwrap();
        assert_eq!(selection.missing_ids, vec![gone_video.id]);
        assert!(selection.skipped_source_paths.is_empty());
    }

    /// 同源只有一个全部文件被删除的视频时，因没有任何文件可确认为证据而整源保留，
    /// 并通过 skipped_source_paths 说明该限制（宁可保留可疑的真实缺失，也不能误删）
    #[tokio::test]
    async fn single_deleted_video_without_evidence_is_kept() {
        let dir = temp_root("cleanup-single-deleted");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1single", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        std::fs::remove_dir_all(&video_dir).unwrap();

        let selection = select_cleanup_targets(&db).await.unwrap();
        assert!(selection.missing_ids.is_empty());
        assert_eq!(
            selection.skipped_source_paths,
            vec![source_dir.to_string_lossy().to_string()]
        );
    }

    /// B 站失效记录无论文件是否存在都进入 invalid 候选
    #[tokio::test]
    async fn invalid_video_selected() {
        let dir = temp_root("cleanup-invalid");
        let db = test_db(&dir).await;
        let source = insert_source(&db, &dir.join("source")).await;
        let video = insert_video(&db, source.id, "BV1invalid", &dir.join("source").join("v"), false, true).await;
        insert_page(
            &db,
            video.id,
            0,
            Some(&dir.join("source").join("v").join("0.mp4")),
            page_status(7),
        )
        .await;
        let (invalid, missing) = select(&db).await;
        assert_eq!(invalid, vec![video.id]);
        assert!(missing.is_empty());
    }

    /// 已下载完成且全部分 P 文件均不存在时进入 missing 候选
    #[tokio::test]
    async fn missing_video_selected() {
        let dir = temp_root("cleanup-missing");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        // 同源保留一个文件存在的视频作为「磁盘已挂载」证据
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1misskeep", &keep_dir, true, true).await;
        touch(&keep_dir.join("0.mp4"));
        insert_page(&db, keep_video.id, 0, Some(&keep_dir.join("0.mp4")), page_status(7)).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1missing", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        insert_page(&db, video.id, 1, Some(&video_dir.join("1.mp4")), page_status(7)).await;
        let (invalid, missing) = select(&db).await;
        assert!(invalid.is_empty());
        assert_eq!(missing, vec![video.id]);
    }

    /// 分页下载失败达到最大重试（0b100）时，即使视频整体已完成、文件不存在也不按本地缺失清理
    #[tokio::test]
    async fn all_pages_failed_keeps_video() {
        let dir = temp_root("cleanup-allfailed");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1allfailed", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(4)).await;
        let (invalid, missing) = select(&db).await;
        assert!(invalid.is_empty());
        assert!(missing.is_empty());
    }

    /// 存在任一失败分页时整条记录保守跳过，即使其余分页成功
    #[tokio::test]
    async fn partial_failed_page_keeps_video() {
        let dir = temp_root("cleanup-partialfailed");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1partialfailed", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        insert_page(&db, video.id, 1, Some(&video_dir.join("1.mp4")), page_status(4)).await;
        let (_, missing) = select(&db).await;
        assert!(missing.is_empty());
    }

    /// 部分文件仍存在时不按本地缺失清理
    #[tokio::test]
    async fn partial_file_keeps_video() {
        let dir = temp_root("cleanup-partial");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1partial", &video_dir, true, true).await;
        touch(&video_dir.join("0.mp4"));
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        insert_page(&db, video.id, 1, Some(&video_dir.join("1.mp4")), page_status(7)).await;
        let (_, missing) = select(&db).await;
        assert!(missing.is_empty());
    }

    /// 用户删除整个视频目录（视频目录明确 NotFound）时应识别为文件缺失并清理
    #[tokio::test]
    async fn whole_video_dir_deleted_selected() {
        let dir = temp_root("cleanup-whole-dir");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        // 同源保留一个文件存在的视频作为「磁盘已挂载」证据
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1whokeep", &keep_dir, true, true).await;
        touch(&keep_dir.join("0.mp4"));
        insert_page(&db, keep_video.id, 0, Some(&keep_dir.join("0.mp4")), page_status(7)).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1wholedir", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        std::fs::remove_dir_all(&video_dir).unwrap();
        let (invalid, missing) = select(&db).await;
        assert!(invalid.is_empty());
        assert_eq!(missing, vec![video.id]);
    }

    /// 源目录不可访问（权限错误）时不按本地缺失清理
    #[cfg(unix)]
    #[tokio::test]
    async fn source_dir_permission_error_keeps_video() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_root("cleanup-perm");
        let outer = dir.join("outer");
        let source_dir = outer.join("inner");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1perm", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        // 移除源目录父级的可执行权限，使源目录路径 stat 返回权限错误；测试结束前恢复
        std::fs::set_permissions(&outer, std::fs::Permissions::from_mode(0o000)).unwrap();
        let (_, missing) = select(&db).await;
        std::fs::set_permissions(&outer, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(missing.is_empty());
    }

    /// 源目录不可访问（不存在）时不按本地缺失清理
    #[tokio::test]
    async fn missing_source_dir_keeps_video() {
        let dir = temp_root("cleanup-sourcedir");
        let db = test_db(&dir).await;
        let source = insert_source(&db, &dir.join("not-exist")).await;
        let video_dir = dir.join("elsewhere").join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1sourcedir", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        let (_, missing) = select(&db).await;
        assert!(missing.is_empty());
    }

    /// 未下载完成的记录不按本地缺失清理
    #[tokio::test]
    async fn unfinished_video_kept() {
        let dir = temp_root("cleanup-unfinished");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1unfinished", &video_dir, true, false).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        let (_, missing) = select(&db).await;
        assert!(missing.is_empty());
    }

    /// 分页路径缺失（无法核验文件位置）的记录不清理
    #[tokio::test]
    async fn empty_page_path_keeps_video() {
        let dir = temp_root("cleanup-pagepath");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1pagepath", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, None, page_status(7)).await;
        let (_, missing) = select(&db).await;
        assert!(missing.is_empty());
    }

    /// 执行清理只删除确认过且重新核验通过的候选，其余记录与本地文件均不受影响
    #[tokio::test]
    async fn execute_deletes_records_and_keeps_files() {
        let dir = temp_root("cleanup-execute");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let missing_dir = source_dir.join("missing");
        std::fs::create_dir_all(&missing_dir).unwrap();
        let missing_video = insert_video(&db, source.id, "BV1clean", &missing_dir, true, true).await;
        insert_page(
            &db,
            missing_video.id,
            0,
            Some(&missing_dir.join("0.mp4")),
            page_status(7),
        )
        .await;
        let invalid_video = insert_video(&db, source.id, "BV1invalid", &missing_dir, false, true).await;
        insert_page(
            &db,
            invalid_video.id,
            0,
            Some(&missing_dir.join("invalid.mp4")),
            page_status(7),
        )
        .await;
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1keep", &keep_dir, true, true).await;
        let keep_file = keep_dir.join("0.mp4");
        touch(&keep_file);
        insert_page(&db, keep_video.id, 0, Some(&keep_file), page_status(7)).await;

        let report = execute_cleanup_records(&db, &[invalid_video.id], &[missing_video.id])
            .await
            .unwrap();
        assert_eq!(report.deleted_videos_count, 2);
        assert_eq!(report.deleted_pages_count, 2);
        assert_eq!(report.skipped_count, 0);
        assert!(
            video::Entity::find_by_id(missing_video.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            video::Entity::find_by_id(invalid_video.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            video::Entity::find_by_id(keep_video.id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            page::Entity::find()
                .filter(page::Column::VideoId.is_in([missing_video.id, invalid_video.id]))
                .all(&db)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(keep_file.exists());
    }

    /// 执行时重新核验确认过的候选：预览后文件恢复存在的记录不再删除，计入 skipped
    #[tokio::test]
    async fn execute_reverifies_confirmed_candidates() {
        let dir = temp_root("cleanup-reverify");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1reverify", &video_dir, true, true).await;
        let mp4 = video_dir.join("0.mp4");
        insert_page(&db, video.id, 0, Some(&mp4), page_status(7)).await;
        // 同源保留一个文件存在的视频作为「磁盘已挂载」证据
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1revkeep", &keep_dir, true, true).await;
        touch(&keep_dir.join("0.mp4"));
        insert_page(&db, keep_video.id, 0, Some(&keep_dir.join("0.mp4")), page_status(7)).await;
        let (_, missing) = select(&db).await;
        assert_eq!(missing, vec![video.id]);

        // 预览之后用户恢复了文件，执行时不得删除该记录
        touch(&mp4);
        let report = execute_cleanup_records(&db, &[], &missing).await.unwrap();
        assert_eq!(report.deleted_videos_count, 0);
        assert_eq!(report.skipped_count, 1);
        assert!(video::Entity::find_by_id(video.id).one(&db).await.unwrap().is_some());
    }

    /// 文件缺失记录清理后回退来源扫描水位，重新扫描可重新发现
    #[tokio::test]
    async fn missing_cleanup_rolls_back_watermark() {
        let dir = temp_root("cleanup-rollback-missing");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        update_source_watermark(&db, source.id, datetime((2026, 6, 2), (0, 0, 0))).await;
        // 同源保留一个文件存在的视频作为「磁盘已挂载」证据
        let keep_dir = source_dir.join("keep");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_video = insert_video(&db, source.id, "BV1rolkeep", &keep_dir, true, true).await;
        touch(&keep_dir.join("0.mp4"));
        insert_page(&db, keep_video.id, 0, Some(&keep_dir.join("0.mp4")), page_status(7)).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1rollback", &video_dir, true, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;
        // 将视频的收藏时间（favtime，水位回退的依据）设为 2026-06-01 12:00:00
        set_video_favtime(&db, video.id, datetime((2026, 6, 1), (12, 0, 0))).await;

        execute_cleanup_records(&db, &[], &[video.id]).await.unwrap();
        let source = favorite::Entity::find_by_id(source.id).one(&db).await.unwrap().unwrap();
        assert_eq!(source.latest_row_at, datetime((2026, 6, 1), (11, 59, 59)));
    }

    /// B 站失效记录清理后不回退水位，避免无意义地复活失效记录
    #[tokio::test]
    async fn invalid_cleanup_keeps_watermark() {
        let dir = temp_root("cleanup-rollback-invalid");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let watermark = datetime((2026, 6, 2), (0, 0, 0));
        update_source_watermark(&db, source.id, watermark).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1invalidw", &video_dir, false, true).await;
        insert_page(&db, video.id, 0, Some(&video_dir.join("0.mp4")), page_status(7)).await;

        execute_cleanup_records(&db, &[video.id], &[]).await.unwrap();
        let source = favorite::Entity::find_by_id(source.id).one(&db).await.unwrap().unwrap();
        assert_eq!(source.latest_row_at, watermark);
    }

    fn datetime((y, mo, d): (i32, u32, u32), (h, mi, s): (u32, u32, u32)) -> chrono::NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
    }

    async fn update_source_watermark(db: &DatabaseConnection, id: i32, latest_row_at: chrono::NaiveDateTime) {
        favorite::ActiveModel {
            id: Unchanged(id),
            latest_row_at: Set(latest_row_at),
            ..Default::default()
        }
        .update(db)
        .await
        .unwrap();
    }

    async fn set_video_favtime(db: &DatabaseConnection, id: i32, favtime: chrono::NaiveDateTime) {
        video::ActiveModel {
            id: Unchanged(id),
            favtime: Set(favtime),
            ..Default::default()
        }
        .update(db)
        .await
        .unwrap();
    }

    fn touch(path: &std::path::Path) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, b"").unwrap();
    }
    /// 跨源证据不采信：一个来源的视频分 P 路径残留指向另一个来源目录内的真实文件时：
    /// 该真实文件对 stat 存在，使视频不满足「全部 NotFound」，不会成为候选；
    /// 同时它不属于本源，不能充当本源证据，因此本源的缺失候选仍按「无证据」处理。
    /// 场景直接断言另一个源目录有文件、但本源目录（空）下所有缺失候选整源跳过
    #[tokio::test]
    async fn evidence_outside_source_dir_rejected() {
        let dir = temp_root("cleanup-evidence-outside");
        let source_dir = dir.join("source");
        std::fs::create_dir_all(&source_dir).unwrap();
        // 另一个来源目录带一个真实有效文件
        let other_dir = dir.join("other");
        std::fs::create_dir_all(&other_dir).unwrap();
        touch(&other_dir.join("stale.mp4"));
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1outsider", &video_dir, true, true).await;
        // 分 P 记录的路径指向另一个来源目录内「实际存在」的文件：旧实现会把它当作本源证据，
        // 修复后跨源路径不采信；同时该文件存在令视频不满足全部 NotFound，不成为缺失候选
        insert_page(&db, video.id, 0, Some(&other_dir.join("stale.mp4")), page_status(7)).await;
        // 本源再放一个视频，确实全部文件 NotFound，可作为缺失候选
        let gone_dir = source_dir.join("gone");
        std::fs::create_dir_all(&gone_dir).unwrap();
        let gone_video = insert_video(&db, source.id, "BV1outsider2", &gone_dir, true, true).await;
        insert_page(&db, gone_video.id, 0, Some(&gone_dir.join("0.mp4")), page_status(7)).await;
        std::fs::remove_dir_all(&gone_dir).unwrap();

        let selection = select_cleanup_targets(&db).await.unwrap();
        // 本源内没有可采信文件：整源跳过，文件缺失候选必须保留
        assert!(selection.missing_ids.is_empty());
        assert_eq!(
            selection.skipped_source_paths,
            vec![source_dir.to_string_lossy().to_string()]
        );
        // 另一个来源目录的真实文件不受影响
        assert!(other_dir.join("stale.mp4").exists());
    }

    /// page.path 指向源目录内的一个子目录（不是常规文件）：
    /// 该路径不能作为本源的挂载证据，不参与证据校验
    #[tokio::test]
    async fn evidence_non_regular_file_rejected() {
        let dir = temp_root("cleanup-evidence-nonfile");
        let source_dir = dir.join("source");
        // 源目录下存在一个子目录
        std::fs::create_dir_all(source_dir.join("sub")).unwrap();
        let db = test_db(&dir).await;
        let source = insert_source(&db, &source_dir).await;
        let video_dir = source_dir.join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, "BV1nonfile", &video_dir, true, true).await;
        // page.path 记录的是子目录（对目录 stat 成功，但不是常规文件），
        // 但视频不能依据「目录存在」被认定缺失；子目录不是有效证据。
        // 这里直接用文件不存在 + 路径是子目录来构造「可确认缺失 + 无有效证据」
        insert_page(&db, video.id, 0, Some(&source_dir.join("sub")), page_status(7)).await;
        let selection = select_cleanup_targets(&db).await.unwrap();
        // 目录对 stat 显示存在 -> not missing -> skip 列表为空，本测试只验证目录录入本身不误报缺失
        assert!(selection.missing_ids.is_empty());
        assert!(selection.skipped_source_paths.is_empty());
    }

    #[cfg(unix)]

    /// 悬空符号链接指向的目标消失后不能作为证据

    #[tokio::test]

    async fn evidence_dangling_symlink_rejected() {
        let dir = temp_root("cleanup-evidence-dangling");

        let source_dir = dir.join("source");

        std::fs::create_dir_all(&source_dir).unwrap();

        let db = test_db(&dir).await;

        let source = insert_source(&db, &source_dir).await;

        let video_dir = source_dir.join("v");

        std::fs::create_dir_all(&video_dir).unwrap();

        let video = insert_video(&db, source.id, "BV1dangling", &video_dir, true, true).await;

        #[allow(clippy::let_unit_value)]
        let link = source_dir.join("link.mp4");

        let _ = std::os::unix::fs::symlink(dir.join("no-such-target"), &link);

        insert_page(&db, video.id, 0, Some(&link), page_status(7)).await;

        let selection = select_cleanup_targets(&db).await.unwrap();

        assert!(selection.missing_ids.is_empty());

        assert_eq!(
            selection.skipped_source_paths,
            vec![source_dir.to_string_lossy().to_string()]
        );
    }
}
