use axum::extract::{Extension, Path};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use bili_sync_entity::*;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::api::error::InnerApiError;
use crate::api::request::{OrganizationActionRequest, OrganizationRuleRequest};
use crate::api::response::{
    OrganizationExecuteResponse, OrganizationPlanItem, OrganizationPlanResponse, OrganizationRuleResponse,
    OrganizationRulesResponse,
};
use crate::api::wrapper::{ApiError, ApiResponse, ValidatedJson};
use crate::organization::{self, ItemAction, PlanItem};

pub(super) fn router() -> Router {
    Router::new()
        .route("/organization/rules", get(get_rules).post(create_rule))
        .route("/organization/rules/{id}", put(update_rule).delete(delete_rule))
        .route("/organization/rules/{id}/preview", post(preview_rule))
        .route("/organization/rules/{id}/execute", post(execute_rule))
}

/// 列出全部整理规则
pub async fn get_rules(
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<OrganizationRulesResponse>, ApiError> {
    let rules = organization_rule::Entity::find()
        .order_by_asc(organization_rule::Column::Id)
        .all(&db)
        .await?;
    Ok(ApiResponse::ok(OrganizationRulesResponse {
        rules: rules.into_iter().map(rule_response).collect(),
    }))
}

/// 创建整理规则
pub async fn create_rule(
    Extension(db): Extension<DatabaseConnection>,
    ValidatedJson(request): ValidatedJson<OrganizationRuleRequest>,
) -> Result<ApiResponse<OrganizationRuleResponse>, ApiError> {
    let _guard = organization::execution_lock().lock().await;
    let (source_path, output_path) =
        organization::validate_rule_paths(&db, None, &request.source_path, &request.output_path).await?;
    let rule = organization_rule::ActiveModel {
        name: Set(request.name),
        rule_type: Set(request.rule_type.to_string()),
        source_path: Set(source_path),
        output_path: Set(output_path),
        ..Default::default()
    }
    .insert(&db)
    .await?;
    Ok(ApiResponse::ok(rule_response(rule)))
}

/// 更新整理规则
pub async fn update_rule(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    ValidatedJson(request): ValidatedJson<OrganizationRuleRequest>,
) -> Result<ApiResponse<OrganizationRuleResponse>, ApiError> {
    let _guard = organization::execution_lock().lock().await;
    let Some(rule) = organization_rule::Entity::find_by_id(id).one(&db).await? else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let (source_path, output_path) =
        organization::validate_rule_paths(&db, Some(id), &request.source_path, &request.output_path).await?;
    // 已产生整理记录的规则锁定源目录/输出目录/规则类型，防止已记录归属被用于在新目标覆写
    organization::ensure_rule_editable(&rule, &db, &source_path, &output_path, &request.rule_type.to_string()).await?;
    let mut rule: organization_rule::ActiveModel = rule.into();
    rule.name = Set(request.name);
    rule.rule_type = Set(request.rule_type.to_string());
    rule.source_path = Set(source_path);
    rule.output_path = Set(output_path);
    let rule = rule.update(&db).await?;
    Ok(ApiResponse::ok(rule_response(rule)))
}

/// 删除整理规则，同时清理其整理记录；已生成的文件保留在磁盘上
pub async fn delete_rule(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
) -> Result<ApiResponse<bool>, ApiError> {
    let _guard = organization::execution_lock().lock().await;
    let txn = db.begin().await?;
    let result = organization_rule::Entity::delete_by_id(id).exec(&txn).await?;
    organization_output::Entity::delete_many()
        .filter(organization_output::Column::RuleId.eq(id))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    if result.rows_affected == 0 {
        return Err(InnerApiError::NotFound(id).into());
    }
    Ok(ApiResponse::ok(true))
}

/// 预览规则的整理计划，纯只读
pub async fn preview_rule(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    Json(request): Json<OrganizationActionRequest>,
) -> Result<ApiResponse<OrganizationPlanResponse>, ApiError> {
    let Some(rule) = organization_rule::Entity::find_by_id(id).one(&db).await? else {
        return Err(InnerApiError::NotFound(id).into());
    };
    let plan = organization::preview_rule(&rule, &db, request.video_ids.as_deref()).await?;
    Ok(ApiResponse::ok(OrganizationPlanResponse {
        items: plan.items.iter().map(plan_item_response).collect(),
        warnings: plan.warnings,
    }))
}

/// 执行规则的整理
pub async fn execute_rule(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    Json(request): Json<OrganizationActionRequest>,
) -> Result<ApiResponse<OrganizationExecuteResponse>, ApiError> {
    let rule = organization_rule::Entity::find_by_id(id).one(&db).await?;
    let Some(rule) = rule else {
        return Err(InnerApiError::NotFound(id).into());
    };
    if rule.rule_type == "movie" && request.video_ids.as_ref().is_none_or(Vec::is_empty) {
        return Err(InnerApiError::BadRequest("电影规则执行需要选择视频".to_string()).into());
    }
    // 前提：整理期间没有其他进程移动/替换目录或变更挂载；写入仍有路径校验与绝不覆盖保障。
    let report = organization::execute_rule(&rule, &db, request.video_ids.as_deref()).await?;
    Ok(ApiResponse::ok(OrganizationExecuteResponse {
        created: report.created,
        refreshed: report.refreshed,
        skipped: report.skipped,
        failed: report.failed,
        extras_created: report.extras_created,
        items: report.items.iter().map(plan_item_response).collect(),
        warnings: report.warnings,
    }))
}

fn rule_response(rule: organization_rule::Model) -> OrganizationRuleResponse {
    OrganizationRuleResponse {
        id: rule.id,
        name: rule.name,
        rule_type: rule.rule_type,
        source_path: rule.source_path,
        output_path: rule.output_path,
    }
}

fn plan_item_response(item: &PlanItem) -> OrganizationPlanItem {
    let (action, action_message) = match &item.action {
        ItemAction::Create => ("create", None),
        ItemAction::Refresh => ("refresh", None),
        ItemAction::Exists => ("exists", Some("目标文件已存在且非本规则生成，未覆盖".to_string())),
        ItemAction::Conflict(message) => ("conflict", Some(message.clone())),
        ItemAction::Missing(message) => ("missing", Some(message.clone())),
        ItemAction::Failed(message) => ("failed", Some(message.clone())),
    };
    OrganizationPlanItem {
        video_id: item.video_id,
        page_id: item.page_id,
        bvid: item.bvid.clone(),
        cid: item.cid,
        video_name: item.video_name.clone(),
        page_name: item.page_name.clone(),
        series: item.series.clone(),
        episode: item.episode,
        action: action.to_string(),
        target: item.target.as_ref().map(|target| target.to_string_lossy().to_string()),
        message: item.message.clone().or(action_message),
    }
}
