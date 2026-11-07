#[cfg(test)]
mod tests;

use axum::routing::get;
use axum::{Extension, Router};
use bili_sync_entity::*;
use sea_orm::{DatabaseConnection, EntityTrait, QueryOrder, QuerySelect};

use crate::api::response::{DashBoardResponse, RecentVideoInfo};
use crate::api::wrapper::{ApiError, ApiResponse};

pub(super) fn router() -> Router {
    Router::new().route("/dashboard", get(get_dashboard))
}

async fn get_dashboard(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<DashBoardResponse>, ApiError> {
    // 最近入库：按入库时间（记录新增）倒序取 5 条；id 倒序保证同一批次内新记录优先
    let recent_videos = video::Entity::find()
        .order_by_desc(video::Column::CreatedAt)
        .order_by_desc(video::Column::Id)
        .limit(5)
        .into_partial_model::<RecentVideoInfo>()
        .all(&db)
        .await?;
    Ok(ApiResponse::ok(DashBoardResponse { recent_videos }))
}
