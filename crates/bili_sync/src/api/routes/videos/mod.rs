use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::extract::{Extension, Path, Query};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bili_sync_entity::*;
use chrono::NaiveDateTime;
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{Expr, ExprTrait};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, PaginatorTrait, QueryFilter,
    QueryOrder, Select, TransactionTrait, TryIntoModel,
};

use crate::api::error::InnerApiError;
use crate::api::helper::{update_page_download_status, update_video_download_status};
use crate::api::request::{
    ResetFilteredVideoStatusRequest, ResetVideoStatusRequest, UpdateFilteredVideoStatusRequest,
    UpdateVideoStatusRequest, VideosCleanupRequest, VideosRequest,
};
use crate::api::response::{
    ClearAndResetVideoStatusResponse, PageInfo, ResetFilteredVideosResponse, ResetVideoResponse, SimplePageInfo,
    SimpleVideoInfo, UpdateFilteredVideoStatusResponse, UpdateVideoStatusResponse, VideoInfo, VideoResponse,
    VideosCleanupPreviewResponse, VideosCleanupResponse, VideosResponse,
};
use crate::api::wrapper::{ApiError, ApiResponse, ValidatedJson};
use crate::task::DownloadTaskManager;
use crate::utils::status::{PageStatus, VideoStatus};

mod cleanup;

pub(super) fn router() -> Router {
    Router::new()
        .route("/videos", get(get_videos))
        .route("/videos/{id}", get(get_video).delete(delete_video))
        .route("/videos/{id}/cover", get(get_video_cover))
        .route(
            "/videos/{id}/clear-and-reset-status",
            post(clear_and_reset_video_status),
        )
        .route("/videos/{id}/reset-status", post(reset_video_status))
        .route("/videos/{id}/update-status", post(update_video_status))
        .route("/videos/reset-status", post(reset_filtered_video_status))
        .route("/videos/update-status", post(update_filtered_video_status))
        .route("/videos/cleanup/preview", post(preview_cleanup))
        .route("/videos/cleanup", post(execute_cleanup))
}

fn apply_created_at_filter(
    mut query: Select<video::Entity>,
    created_from: Option<NaiveDateTime>,
    created_to: Option<NaiveDateTime>,
) -> Select<video::Entity> {
    if let Some(created_from) = created_from {
        query = query.filter(
            Expr::cust_with_expr("datetime(?, 'localtime')", Expr::col(video::Column::CreatedAt))
                .gte(created_from.format("%Y-%m-%d %H:%M:%S").to_string()),
        );
    }
    if let Some(created_to) = created_to {
        query = query.filter(
            Expr::cust_with_expr("datetime(?, 'localtime')", Expr::col(video::Column::CreatedAt))
                .lte(created_to.format("%Y-%m-%d %H:%M:%S").to_string()),
        );
    }
    query
}

/// 列出视频的基本信息，支持根据视频来源筛选、名称查找和分页
pub async fn get_videos(
    Extension(db): Extension<DatabaseConnection>,
    Query(params): Query<VideosRequest>,
) -> Result<ApiResponse<VideosResponse>, ApiError> {
    let mut query = video::Entity::find();
    for (field, column) in [
        (params.collection, video::Column::CollectionId),
        (params.favorite, video::Column::FavoriteId),
        (params.submission, video::Column::SubmissionId),
        (params.watch_later, video::Column::WatchLaterId),
    ] {
        if let Some(id) = field {
            query = query.filter(column.eq(id));
        }
    }
    if let Some(query_word) = params.query {
        query = query.filter(
            video::Column::Name
                .contains(&query_word)
                .or(video::Column::Bvid.contains(query_word)),
        );
    }
    if let Some(status_filter) = params.status_filter {
        query = query.filter(status_filter.to_video_query());
    }
    if let Some(validation_filter) = params.validation_filter {
        query = query.filter(validation_filter.to_video_query());
    }
    query = apply_created_at_filter(query, params.created_from, params.created_to);
    let total_count = query.clone().count(&db).await?;
    let (page, page_size) = if let (Some(page), Some(page_size)) = (params.page, params.page_size) {
        (page, page_size)
    } else {
        (0, 10)
    };
    Ok(ApiResponse::ok(VideosResponse {
        videos: query
            .order_by_desc(video::Column::Id)
            .into_partial_model::<VideoInfo>()
            .paginate(&db, page_size)
            .fetch_page(page)
            .await?,
        total_count,
    }))
}

pub async fn get_video(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<VideoResponse>, ApiError> {
    let (video_info, pages_info) = tokio::try_join!(
        video::Entity::find_by_id(id).into_partial_model::<VideoInfo>().one(&db),
        page::Entity::find()
            .filter(page::Column::VideoId.eq(id))
            .order_by_asc(page::Column::Cid)
            .into_partial_model::<PageInfo>()
            .all(&db)
    )?;
    let Some(video_info) = video_info else {
        return Err(InnerApiError::NotFound(id).into());
    };
    Ok(ApiResponse::ok(VideoResponse {
        video: video_info,
        pages: pages_info,
    }))
}

pub async fn reset_video_status(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    Json(request): Json<ResetVideoStatusRequest>,
) -> Result<ApiResponse<ResetVideoResponse>, ApiError> {
    let (video_info, pages_info) = tokio::try_join!(
        video::Entity::find_by_id(id).into_partial_model::<VideoInfo>().one(&db),
        page::Entity::find()
            .filter(page::Column::VideoId.eq(id))
            .order_by_asc(page::Column::Cid)
            .into_partial_model::<PageInfo>()
            .all(&db)
    )?;
    let Some(mut video_info) = video_info else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let resetted_pages_info = pages_info
        .into_iter()
        .filter_map(|mut page_info| {
            let mut page_status = PageStatus::from(page_info.download_status);
            if (request.force && page_status.force_reset_failed()) || page_status.reset_failed() {
                page_info.download_status = page_status.into();
                Some(page_info)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let mut video_status = VideoStatus::from(video_info.download_status);
    let mut video_resetted = (request.force && video_status.force_reset_failed()) || video_status.reset_failed();
    if !resetted_pages_info.is_empty() {
        video_status.set(4, 0); //  将“分页下载”重置为 0
        video_resetted = true;
    }
    let resetted_videos_info = if video_resetted {
        video_info.download_status = video_status.into();
        vec![&video_info]
    } else {
        vec![]
    };
    let resetted = !resetted_videos_info.is_empty() || !resetted_pages_info.is_empty();
    if resetted {
        let txn = db.begin().await?;
        if !resetted_videos_info.is_empty() {
            // 只可能有 1 个元素，所以不用 batch
            update_video_download_status::<VideoInfo>(&txn, &resetted_videos_info, None).await?;
        }
        if !resetted_pages_info.is_empty() {
            update_page_download_status(&txn, &resetted_pages_info, Some(500)).await?;
        }
        txn.commit().await?;
    }
    Ok(ApiResponse::ok(ResetVideoResponse {
        resetted,
        video: video_info,
        pages: resetted_pages_info,
    }))
}

pub async fn clear_and_reset_video_status(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<ClearAndResetVideoStatusResponse>, ApiError> {
    let video_info = video::Entity::find_by_id(id).one(&db).await?;
    let Some(video_info) = video_info else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let txn = db.begin().await?;
    let mut video_info = video_info.into_active_model();
    video_info.single_page = Set(None);
    video_info.download_status = Set(0);
    video_info.valid = Set(true);
    let video_info = video_info.update(&txn).await?;
    page::Entity::delete_many()
        .filter(page::Column::VideoId.eq(id))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    let video_info = video_info.try_into_model()?;
    let warning = if video_info.path.is_empty() {
        None
    } else {
        tokio::fs::remove_dir_all(&video_info.path)
            .await
            .context(format!("删除本地路径「{}」失败", video_info.path))
            .err()
            .map(|e| format!("{:#}", e))
    };
    Ok(ApiResponse::ok(ClearAndResetVideoStatusResponse {
        warning,
        video: VideoInfo {
            id: video_info.id,
            bvid: video_info.bvid,
            name: video_info.name,
            upper_name: video_info.upper_name,
            valid: video_info.valid,
            should_download: video_info.should_download,
            download_status: video_info.download_status,
            collection_id: video_info.collection_id,
            favorite_id: video_info.favorite_id,
            submission_id: video_info.submission_id,
            watch_later_id: video_info.watch_later_id,
        },
    }))
}

pub async fn reset_filtered_video_status(
    Extension(db): Extension<DatabaseConnection>,
    Json(request): Json<ResetFilteredVideoStatusRequest>,
) -> Result<ApiResponse<ResetFilteredVideosResponse>, ApiError> {
    let mut query = video::Entity::find();
    for (field, column) in [
        (request.collection, video::Column::CollectionId),
        (request.favorite, video::Column::FavoriteId),
        (request.submission, video::Column::SubmissionId),
        (request.watch_later, video::Column::WatchLaterId),
    ] {
        if let Some(id) = field {
            query = query.filter(column.eq(id));
        }
    }
    if let Some(query_word) = request.query {
        query = query.filter(
            video::Column::Name
                .contains(&query_word)
                .or(video::Column::Bvid.contains(query_word)),
        );
    }
    if let Some(status_filter) = request.status_filter {
        query = query.filter(status_filter.to_video_query());
    }
    if let Some(validation_filter) = request.validation_filter {
        query = query.filter(validation_filter.to_video_query());
    }
    query = apply_created_at_filter(query, request.created_from, request.created_to);
    let all_videos = query.into_partial_model::<SimpleVideoInfo>().all(&db).await?;
    let all_pages = page::Entity::find()
        .filter(page::Column::VideoId.is_in(all_videos.iter().map(|v| v.id)))
        .into_partial_model::<SimplePageInfo>()
        .all(&db)
        .await?;
    let resetted_pages_info = all_pages
        .into_iter()
        .filter_map(|mut page_info| {
            let mut page_status = PageStatus::from(page_info.download_status);
            if (request.force && page_status.force_reset_failed()) || page_status.reset_failed() {
                page_info.download_status = page_status.into();
                Some(page_info)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let video_ids_with_resetted_pages: HashSet<i32> = resetted_pages_info.iter().map(|page| page.video_id).collect();
    let resetted_videos_info = all_videos
        .into_iter()
        .filter_map(|mut video_info| {
            let mut video_status = VideoStatus::from(video_info.download_status);
            let mut video_resetted =
                (request.force && video_status.force_reset_failed()) || video_status.reset_failed();
            if video_ids_with_resetted_pages.contains(&video_info.id) {
                video_status.set(4, 0); // 将"分页下载"重置为 0
                video_resetted = true;
            }
            if video_resetted {
                video_info.download_status = video_status.into();
                Some(video_info)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    let has_video_updates = !resetted_videos_info.is_empty();
    let has_page_updates = !resetted_pages_info.is_empty();
    if has_video_updates || has_page_updates {
        let txn = db.begin().await?;
        if has_video_updates {
            update_video_download_status(&txn, &resetted_videos_info, Some(500)).await?;
        }
        if has_page_updates {
            update_page_download_status(&txn, &resetted_pages_info, Some(500)).await?;
        }
        txn.commit().await?;
    }
    Ok(ApiResponse::ok(ResetFilteredVideosResponse {
        resetted: has_video_updates || has_page_updates,
        resetted_videos_count: resetted_videos_info.len(),
        resetted_pages_count: resetted_pages_info.len(),
    }))
}

pub async fn update_video_status(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    ValidatedJson(request): ValidatedJson<UpdateVideoStatusRequest>,
) -> Result<ApiResponse<UpdateVideoStatusResponse>, ApiError> {
    let (video_info, mut pages_info) = tokio::try_join!(
        video::Entity::find_by_id(id).into_partial_model::<VideoInfo>().one(&db),
        page::Entity::find()
            .filter(page::Column::VideoId.eq(id))
            .order_by_asc(page::Column::Cid)
            .into_partial_model::<PageInfo>()
            .all(&db)
    )?;
    let Some(mut video_info) = video_info else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let mut video_status = VideoStatus::from(video_info.download_status);
    for update in &request.video_updates {
        video_status.set(update.status_index, update.status_value);
    }
    video_info.download_status = video_status.into();
    let mut updated_pages_info = Vec::new();
    let mut page_id_map = pages_info
        .iter_mut()
        .map(|page| (page.id, page))
        .collect::<std::collections::HashMap<_, _>>();
    for page_update in &request.page_updates {
        if let Some(page_info) = page_id_map.remove(&page_update.page_id) {
            let mut page_status = PageStatus::from(page_info.download_status);
            for update in &page_update.updates {
                page_status.set(update.status_index, update.status_value);
            }
            page_info.download_status = page_status.into();
            updated_pages_info.push(page_info);
        }
    }
    let has_video_updates = !request.video_updates.is_empty();
    let has_page_updates = !updated_pages_info.is_empty();
    if has_video_updates || has_page_updates {
        let txn = db.begin().await?;
        if has_video_updates {
            update_video_download_status::<VideoInfo>(&txn, &[&video_info], None).await?;
        }
        if has_page_updates {
            update_page_download_status::<PageInfo>(&txn, &updated_pages_info, None).await?;
        }
        txn.commit().await?;
    }
    Ok(ApiResponse::ok(UpdateVideoStatusResponse {
        success: has_video_updates || has_page_updates,
        video: video_info,
        pages: pages_info,
    }))
}

pub async fn update_filtered_video_status(
    Extension(db): Extension<DatabaseConnection>,
    ValidatedJson(request): ValidatedJson<UpdateFilteredVideoStatusRequest>,
) -> Result<ApiResponse<UpdateFilteredVideoStatusResponse>, ApiError> {
    let mut query = video::Entity::find();
    for (field, column) in [
        (request.collection, video::Column::CollectionId),
        (request.favorite, video::Column::FavoriteId),
        (request.submission, video::Column::SubmissionId),
        (request.watch_later, video::Column::WatchLaterId),
    ] {
        if let Some(id) = field {
            query = query.filter(column.eq(id));
        }
    }
    if let Some(query_word) = request.query {
        query = query.filter(
            video::Column::Name
                .contains(&query_word)
                .or(video::Column::Bvid.contains(query_word)),
        );
    }
    if let Some(status_filter) = request.status_filter {
        query = query.filter(status_filter.to_video_query());
    }
    if let Some(validation_filter) = request.validation_filter {
        query = query.filter(validation_filter.to_video_query());
    }
    query = apply_created_at_filter(query, request.created_from, request.created_to);
    let mut all_videos = query.into_partial_model::<SimpleVideoInfo>().all(&db).await?;
    let mut all_pages = page::Entity::find()
        .filter(page::Column::VideoId.is_in(all_videos.iter().map(|v| v.id)))
        .into_partial_model::<SimplePageInfo>()
        .all(&db)
        .await?;
    for video_info in all_videos.iter_mut() {
        let mut video_status = VideoStatus::from(video_info.download_status);
        for update in &request.video_updates {
            video_status.set(update.status_index, update.status_value);
        }
        video_info.download_status = video_status.into();
    }
    for page_info in all_pages.iter_mut() {
        let mut page_status = PageStatus::from(page_info.download_status);
        for update in &request.page_updates {
            page_status.set(update.status_index, update.status_value);
        }
        page_info.download_status = page_status.into();
    }
    let has_video_updates = !all_videos.is_empty();
    let has_page_updates = !all_pages.is_empty();
    if has_video_updates || has_page_updates {
        let txn = db.begin().await?;
        if has_video_updates {
            update_video_download_status(&txn, &all_videos, Some(500)).await?;
        }
        if has_page_updates {
            update_page_download_status(&txn, &all_pages, Some(500)).await?;
        }
        txn.commit().await?;
    }
    Ok(ApiResponse::ok(UpdateFilteredVideoStatusResponse {
        success: has_video_updates || has_page_updates,
        updated_videos_count: all_videos.len(),
        updated_pages_count: all_pages.len(),
    }))
}

/// 返回视频的本地封面图片；文件缺失或视频未下载时返回 404，由前端展示占位。
/// 路径完全由数据库记录推导，不接受任何用户提供的路径参数，且对结果做目录边界校验。
pub async fn get_video_cover(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<Response, ApiError> {
    let Some(video) = video::Entity::find_by_id(id).one(&db).await? else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let Some(cover) = find_local_cover(&video, &db).await else {
        return Ok(ApiResponse::<()>::not_found("本地封面不存在").into_response());
    };
    let mime = match cover.extension().and_then(|ext| ext.to_str()) {
        Some("png") => "image/png",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        _ => "image/jpeg",
    };
    let bytes = tokio::fs::read(&cover).await?;
    Ok(([(header::CONTENT_TYPE, mime)], bytes).into_response())
}

/// 定位视频的本地封面：多页视频为视频目录下的 poster.jpg，单页视频为分页 mp4 同名的 -poster.jpg
async fn find_local_cover(video: &video::Model, db: &DatabaseConnection) -> Option<PathBuf> {
    if video.path.is_empty() {
        return None;
    }
    let candidate = if video.single_page == Some(true) {
        let page = page::Entity::find()
            .filter(page::Column::VideoId.eq(video.id))
            .order_by_asc(page::Column::Pid)
            .one(db)
            .await
            .ok()??;
        let mp4 = PathBuf::from(page.path?);
        mp4.parent()?
            .join(format!("{}-poster.jpg", mp4.file_stem()?.to_string_lossy()))
    } else {
        PathBuf::from(&video.path).join("poster.jpg")
    };
    // 防御数据库中的异常路径：规范化后确认封面仍位于视频目录内
    let base = dunce::canonicalize(&video.path).ok()?;
    let canonical = dunce::canonicalize(candidate).ok()?;
    canonical.starts_with(base).then_some(canonical)
}

/// 删除单条视频记录及其全部分页记录，保留本地文件；回退所属视频源的扫描水位，
/// 保证重新扫描后能够重新发现该视频并重新下载
pub async fn delete_video(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<bool>, ApiError> {
    let Some(video_info) = video::Entity::find_by_id(id).one(&db).await? else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let _running_lock = acquire_record_mutation_lock().await?;
    let txn = db.begin().await?;
    page::Entity::delete_many()
        .filter(page::Column::VideoId.eq(id))
        .exec(&txn)
        .await?;
    video::Entity::delete_by_id(id).exec(&txn).await?;
    cleanup::rollback_source_latest_row_at(&txn, &video_info).await?;
    txn.commit().await?;
    Ok(ApiResponse::ok(true))
}

/// 预览可批量清理的候选：B 站失效记录，以及已下载完成且全部分 P 文件均不存在的记录。
/// 返回候选 ID，前端展示并让用户确认后原样回传给执行接口
pub async fn preview_cleanup(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<VideosCleanupPreviewResponse>, ApiError> {
    let selection = cleanup::select_cleanup_targets(&db).await?;
    Ok(ApiResponse::ok(VideosCleanupPreviewResponse {
        invalid_ids: selection.invalid_ids,
        missing_ids: selection.missing_ids,
        skipped_source_paths: selection.skipped_source_paths,
    }))
}

/// 执行批量清理：只删除用户确认过且重新核验通过的数据库记录及其分页记录，不删除任何本地文件；
/// 预览到执行之间状态变化的候选会被跳过而不会误删
pub async fn execute_cleanup(
    Extension(db): Extension<DatabaseConnection>,
    ValidatedJson(request): ValidatedJson<VideosCleanupRequest>,
) -> Result<ApiResponse<VideosCleanupResponse>, ApiError> {
    let _running_lock = acquire_record_mutation_lock().await?;
    let report = cleanup::execute_cleanup_records(&db, &request.invalid_ids, &request.missing_ids).await?;
    Ok(ApiResponse::ok(VideosCleanupResponse {
        deleted_videos_count: report.deleted_videos_count,
        deleted_pages_count: report.deleted_pages_count,
        skipped_count: report.skipped_count,
    }))
}

/// 删除/清理记录前与下载任务互斥：下载进行中时拒绝操作，避免已删除的记录被下载流程回写为不完整数据；
/// 任务管理器尚未初始化时不可能有下载任务在运行，直接放行
async fn acquire_record_mutation_lock() -> Result<Option<tokio::sync::MutexGuard<'static, ()>>, ApiError> {
    match DownloadTaskManager::try_get() {
        None => Ok(None),
        Some(manager) => manager.try_running_lock().map(Some).ok_or_else(|| {
            InnerApiError::BadRequest("视频下载任务正在进行中，请等待本轮任务结束后再删除或清理记录".to_string()).into()
        }),
    }
}

#[cfg(test)]
mod tests {
    use bili_sync_migration::{Migrator, MigratorTrait};

    use super::*;

    /// 测试临时根：优先取环境变量 BILI_SYNC_ORG_TEST_ROOT（指向已登记的任务临时根），缺省为系统临时目录；
    /// 在其下用 UUID 创建唯一、不可预测的子目录。测试绝不删除任何目录，
    /// 统一由登记的清理流程或系统临时目录策略回收。
    fn temp_root(name: &str) -> PathBuf {
        let base = std::env::var("BILI_SYNC_ORG_TEST_ROOT")
            .map(PathBuf::from)
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

    fn datetime((y, mo, d): (i32, u32, u32), (h, mi, s): (u32, u32, u32)) -> NaiveDateTime {
        chrono::NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_opt(h, mi, s)
            .unwrap()
    }

    async fn insert_favorite(
        db: &DatabaseConnection,
        path: &std::path::Path,
        latest_row_at: NaiveDateTime,
    ) -> favorite::Model {
        favorite::ActiveModel {
            f_id: Set(1),
            name: Set("测试收藏夹".to_string()),
            path: Set(path.to_string_lossy().to_string()),
            enabled: Set(true),
            latest_row_at: Set(latest_row_at),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    async fn insert_video(
        db: &DatabaseConnection,
        favorite_id: i32,
        favtime: NaiveDateTime,
        path: &std::path::Path,
    ) -> video::Model {
        video::ActiveModel {
            bvid: Set("BV1test".to_string()),
            favorite_id: Set(Some(favorite_id)),
            upper_id: Set(1),
            upper_name: Set("UP主".to_string()),
            upper_face: Set(String::new()),
            intro: Set(String::new()),
            cover: Set(String::new()),
            name: Set("测试视频".to_string()),
            path: Set(path.to_string_lossy().to_string()),
            category: Set(2),
            download_status: Set(0),
            valid: Set(true),
            favtime: Set(favtime),
            pubtime: Set(favtime),
            ctime: Set(favtime),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    async fn insert_page(db: &DatabaseConnection, video_id: i32, mp4: &std::path::Path) -> page::Model {
        page::ActiveModel {
            video_id: Set(video_id),
            cid: Set(1),
            pid: Set(0),
            name: Set("分P0".to_string()),
            duration: Set(0),
            path: Set(Some(mp4.to_string_lossy().to_string())),
            download_status: Set(0),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
        .try_into_model()
        .unwrap()
    }

    /// 删除记录后视频与分页被移除、本地文件保留、来源扫描水位回退到发布时间之前
    #[tokio::test]
    async fn delete_video_keeps_files_and_rolls_back_watermark() {
        let dir = temp_root("video-delete");
        let db = test_db(&dir).await;
        let source = insert_favorite(&db, &dir.join("source"), datetime((2026, 6, 2), (0, 0, 0))).await;
        let video_dir = dir.join("source").join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let mp4 = video_dir.join("0.mp4");
        std::fs::write(&mp4, b"").unwrap();
        let video = insert_video(&db, source.id, datetime((2026, 6, 1), (12, 0, 0)), &video_dir).await;
        let page = insert_page(&db, video.id, &mp4).await;

        let Ok(response) = delete_video(Path(video.id), Extension(db.clone())).await else {
            panic!("删除应成功");
        };
        assert_eq!(response.into_response().status(), axum::http::StatusCode::OK);

        assert!(video::Entity::find_by_id(video.id).one(&db).await.unwrap().is_none());
        assert!(page::Entity::find_by_id(page.id).one(&db).await.unwrap().is_none());
        assert!(mp4.exists());
        let source = favorite::Entity::find_by_id(source.id).one(&db).await.unwrap().unwrap();
        assert_eq!(source.latest_row_at, datetime((2026, 6, 1), (11, 59, 59)));
    }

    /// 扫描水位已经早于回退目标时保持不变，只回退不前移
    #[tokio::test]
    async fn delete_video_keeps_older_watermark() {
        let dir = temp_root("video-delete-older");
        let db = test_db(&dir).await;
        let source = insert_favorite(&db, &dir.join("source"), datetime((2020, 1, 1), (0, 0, 0))).await;
        let video_dir = dir.join("source").join("v");
        std::fs::create_dir_all(&video_dir).unwrap();
        let video = insert_video(&db, source.id, datetime((2026, 6, 1), (12, 0, 0)), &video_dir).await;
        insert_page(&db, video.id, &video_dir.join("0.mp4")).await;

        let Ok(response) = delete_video(Path(video.id), Extension(db.clone())).await else {
            panic!("删除应成功");
        };
        assert_eq!(response.into_response().status(), axum::http::StatusCode::OK);

        let source = favorite::Entity::find_by_id(source.id).one(&db).await.unwrap().unwrap();
        assert_eq!(source.latest_row_at, datetime((2020, 1, 1), (0, 0, 0)));
    }

    /// 删除不存在的记录返回 NotFound
    #[tokio::test]
    async fn delete_missing_video_returns_not_found() {
        let dir = temp_root("video-delete-404");
        let db = test_db(&dir).await;
        let Err(err) = delete_video(Path(999), Extension(db.clone())).await else {
            panic!("应返回 NotFound");
        };
        assert_eq!(err.into_response().status(), axum::http::StatusCode::NOT_FOUND);
    }
}
