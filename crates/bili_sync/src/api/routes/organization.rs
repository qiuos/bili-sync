use std::path::PathBuf;

use anyhow::Context;
use axum::extract::{Extension, Path, Query};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use bili_sync_entity::*;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, TransactionTrait,
};

use crate::api::error::InnerApiError;
use crate::api::request::{
    BrowseDirectoryRequest, OrganizationActionRequest, OrganizationRuleRequest, OrganizationSwitchAggregationRequest,
    OrganizationSwitchPendingAction,
};
use crate::api::response::{
    DirectoryBrowseResponse, DirectoryEntry, OrganizationExecuteResponse, OrganizationPlanItem,
    OrganizationPlanResponse, OrganizationRuleResponse, OrganizationRulesResponse,
    OrganizationSwitchAggregationResponse,
};
use crate::api::wrapper::{ApiError, ApiResponse, ValidatedJson};
use crate::organization::{self, ItemAction, PlanItem};

pub(super) fn router() -> Router {
    Router::new()
        .route("/organization/rules", get(get_rules).post(create_rule))
        .route("/organization/rules/{id}", put(update_rule).delete(delete_rule))
        .route("/organization/rules/{id}/preview", post(preview_rule))
        .route("/organization/rules/{id}/execute", post(execute_rule))
        .route("/organization/rules/{id}/switch_aggregation", post(switch_aggregation))
        .route("/organization/directory", get(browse_directory))
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
    let mode = request.aggregation_mode.unwrap_or_default().to_string();
    organization::validate_aggregation_mode(&request.rule_type.to_string(), &mode)?;
    let rule = organization_rule::ActiveModel {
        name: Set(request.name),
        rule_type: Set(request.rule_type.to_string()),
        aggregation_mode: Set(mode),
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
    // 已产生整理记录的规则锁定源目录/输出目录/规则类型/聚合方式，防止已记录归属被用于在新目标覆写；
    // 聚合方式变更必须走 switch_aggregation 端点（带清理重建与确认）
    let new_aggregation_mode = request
        .aggregation_mode
        .map(|mode| mode.to_string())
        .unwrap_or_else(|| rule.aggregation_mode.clone());
    organization::validate_aggregation_mode(&request.rule_type.to_string(), &new_aggregation_mode)?;
    organization::ensure_rule_editable(
        &rule,
        &db,
        &source_path,
        &output_path,
        &request.rule_type.to_string(),
        &new_aggregation_mode,
    )
    .await?;
    let mut rule: organization_rule::ActiveModel = rule.into();
    rule.name = Set(request.name);
    rule.rule_type = Set(request.rule_type.to_string());
    rule.aggregation_mode = Set(new_aggregation_mode);
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

/// 切换电视剧规则的聚合方式：需显式确认；删除可证明归属的旧整理文件与记录后按新模式重整。
/// 未知归属文件保留并在 retained_paths 报告；失败/中断保留 pending 状态，整理执行被拒绝，
/// 可用 pending_action: retry 续作目标模式。
pub async fn switch_aggregation(
    Path(id): Path<i32>,
    Extension(db): Extension<DatabaseConnection>,
    Json(request): Json<OrganizationSwitchAggregationRequest>,
) -> Result<ApiResponse<OrganizationSwitchAggregationResponse>, ApiError> {
    if let Err(e) = request.ensure_confirmed() {
        return Err(InnerApiError::BadRequest(format!("{:#}", e)).into());
    }
    let target = request.aggregation_mode.to_string();
    let pending = organization_rule::Entity::find_by_id(id)
        .one(&db)
        .await?
        .and_then(|rule| rule.pending_aggregation_mode);
    let report = match (pending, request.pending_action) {
        // 上次切换未完成：仅允许按同一目标 retry 续作
        (Some(pending), Some(OrganizationSwitchPendingAction::Retry)) => {
            if pending != target {
                return Err(InnerApiError::BadRequest(format!(
                    "进行中的切换目标是「{pending}」，无法切换到「{target}」；请先完成该切换"
                ))
                .into());
            }
            organization::resume_pending_switch(id, &db).await?
        }
        (Some(pending), None) => {
            return Err(InnerApiError::BadRequest(format!(
                "规则上次切换未完成（目标 {pending}），请用 pending_action: \"retry\" 重试"
            ))
            .into());
        }
        (None, Some(OrganizationSwitchPendingAction::Retry)) => {
            return Err(InnerApiError::BadRequest("规则没有进行中的聚合方式切换".to_string()).into());
        }
        (None, None) => organization::switch_aggregation_mode(id, &db, &target).await?,
    };
    Ok(ApiResponse::ok(switch_response(report)))
}

fn switch_response(report: organization::SwitchAggregationReport) -> OrganizationSwitchAggregationResponse {
    OrganizationSwitchAggregationResponse {
        records_removed: report.records_removed,
        files_removed: report.files_removed,
        retained_paths: report.retained_paths,
        execute: OrganizationExecuteResponse {
            created: report.execute.created,
            refreshed: report.execute.refreshed,
            skipped: report.execute.skipped,
            failed: report.execute.failed,
            extras_created: report.execute.extras_created,
            items: report.execute.items.iter().map(plan_item_response).collect(),
            warnings: report.execute.warnings,
        },
    }
}

/// 浏览服务端目录，返回子目录列表，供媒体整理规则选择源/输出目录使用。
/// 经 /api 统一认证保护；仅暴露目录名与绝对路径，不返回文件或目录内容。
/// 路径非法、不存在、无法读取等均以 400 返回，便于用户修正输入
pub async fn browse_directory(
    Query(request): Query<BrowseDirectoryRequest>,
) -> Result<ApiResponse<DirectoryBrowseResponse>, ApiError> {
    // ponytail: 空路径仅解析为 "/"，暂不支持 Windows 盘符枚举；部署环境为 Linux 容器
    let start = match request.path.as_deref() {
        None | Some("") => PathBuf::from("/"),
        Some(path) => PathBuf::from(path),
    };
    let response = list_directories(&start).map_err(|e| InnerApiError::BadRequest(format!("{:#}", e)))?;
    Ok(ApiResponse::ok(response))
}

/// 列出指定目录下的全部子目录（含指向目录的符号链接，按名称排序）；根目录没有父目录
fn list_directories(path: &std::path::Path) -> anyhow::Result<DirectoryBrowseResponse> {
    anyhow::ensure!(path.is_absolute(), "路径必须是绝对路径：{}", path.display());
    // 解析符号链接与相对组件，避免在浏览入口暴露未规范化的路径
    let canonical = dunce::canonicalize(path).with_context(|| format!("路径不存在或无法访问：{}", path.display()))?;
    anyhow::ensure!(canonical.is_dir(), "路径不是目录：{}", canonical.display());
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&canonical).with_context(|| format!("目录读取失败：{}", canonical.display()))?
    {
        let entry = entry.with_context(|| format!("目录读取失败：{}", canonical.display()))?;
        // metadata 跟随符号链接：指向目录的链接可进入浏览，断链或指向文件的条目直接跳过
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        entries.push(DirectoryEntry {
            path: canonical.join(entry.file_name()).to_string_lossy().to_string(),
            name,
        });
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(DirectoryBrowseResponse {
        path: canonical.to_string_lossy().to_string(),
        parent: canonical.parent().map(|p| p.to_string_lossy().to_string()),
        entries,
    })
}

fn rule_response(rule: organization_rule::Model) -> OrganizationRuleResponse {
    OrganizationRuleResponse {
        id: rule.id,
        name: rule.name,
        rule_type: rule.rule_type,
        aggregation_mode: rule.aggregation_mode,
        pending_aggregation_mode: rule.pending_aggregation_mode,
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

#[cfg(test)]
mod tests {
    use std::path::Path;

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

    /// 目录浏览仅返回子目录且按名称排序，文件与断链不出现，父目录指向正确
    #[test]
    fn list_directories_returns_sorted_subdirs_only() {
        let root = temp_root("org-browse");
        std::fs::create_dir(root.join("b-dir")).unwrap();
        std::fs::create_dir(root.join("a-dir")).unwrap();
        std::fs::write(root.join("file.txt"), b"").unwrap();
        // 断链不应出现在列表中
        #[cfg(unix)]
        std::os::unix::fs::symlink(root.join("not-exist"), root.join("broken-link")).unwrap();

        let canonical = dunce::canonicalize(&root).unwrap();
        let response = list_directories(&root).unwrap();
        assert_eq!(response.path, canonical.to_string_lossy().to_string());
        assert_eq!(
            response.parent.as_deref(),
            canonical.parent().map(|p| p.to_string_lossy().to_string()).as_deref()
        );
        assert_eq!(
            response.entries.iter().map(|e| e.name.as_str()).collect::<Vec<_>>(),
            vec!["a-dir", "b-dir"]
        );
        assert_eq!(
            response.entries[0].path,
            canonical.join("a-dir").to_string_lossy().to_string()
        );
    }

    /// 相对路径与不存在的路径均返回错误
    #[test]
    fn list_directories_rejects_invalid_paths() {
        assert!(list_directories(Path::new("relative/path")).is_err());
        assert!(list_directories(&temp_root("org-browse-missing").join("not-exist")).is_err());
    }

    /// 根目录没有父目录
    #[cfg(unix)]
    #[test]
    fn list_directories_root_has_no_parent() {
        let response = list_directories(Path::new("/")).unwrap();
        assert!(response.parent.is_none());
    }
}
