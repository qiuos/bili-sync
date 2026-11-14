use bili_sync_entity::rule::Rule;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::bilibili::{CollectionType, FilterOption};

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StatusFilter {
    Failed,
    Succeeded,
    Waiting,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationFilter {
    Skipped,
    Invalid,
    Normal,
}

#[derive(Deserialize)]
pub struct VideosRequest {
    pub collection: Option<i32>,
    pub favorite: Option<i32>,
    pub submission: Option<i32>,
    pub watch_later: Option<i32>,
    pub query: Option<String>,
    pub status_filter: Option<StatusFilter>,
    pub validation_filter: Option<ValidationFilter>,
    pub created_from: Option<NaiveDateTime>,
    pub created_to: Option<NaiveDateTime>,
    pub page: Option<u64>,
    pub page_size: Option<u64>,
}

#[derive(Deserialize)]
pub struct ResetVideoStatusRequest {
    #[serde(default)]
    pub force: bool,
}

#[derive(Deserialize)]
pub struct ResetFilteredVideoStatusRequest {
    pub collection: Option<i32>,
    pub favorite: Option<i32>,
    pub submission: Option<i32>,
    pub watch_later: Option<i32>,
    pub query: Option<String>,
    pub status_filter: Option<StatusFilter>,
    pub validation_filter: Option<ValidationFilter>,
    pub created_from: Option<NaiveDateTime>,
    pub created_to: Option<NaiveDateTime>,
    #[serde(default)]
    pub force: bool,
}

#[derive(Deserialize, Validate)]
pub struct StatusUpdate {
    #[validate(range(min = 0, max = 4))]
    pub status_index: usize,
    #[validate(custom(function = "crate::utils::validation::validate_status_value"))]
    pub status_value: u32,
}

#[derive(Deserialize, Validate)]
pub struct PageStatusUpdate {
    pub page_id: i32,
    #[validate(nested)]
    pub updates: Vec<StatusUpdate>,
}

#[derive(Deserialize, Validate)]
pub struct UpdateVideoStatusRequest {
    #[serde(default)]
    #[validate(nested)]
    pub video_updates: Vec<StatusUpdate>,
    #[serde(default)]
    #[validate(nested)]
    pub page_updates: Vec<PageStatusUpdate>,
}

#[derive(Deserialize, Validate)]
pub struct UpdateFilteredVideoStatusRequest {
    pub collection: Option<i32>,
    pub favorite: Option<i32>,
    pub submission: Option<i32>,
    pub watch_later: Option<i32>,
    pub query: Option<String>,
    pub status_filter: Option<StatusFilter>,
    pub validation_filter: Option<ValidationFilter>,
    pub created_from: Option<NaiveDateTime>,
    pub created_to: Option<NaiveDateTime>,
    #[serde(default)]
    #[validate(nested)]
    pub video_updates: Vec<StatusUpdate>,
    #[serde(default)]
    #[validate(nested)]
    pub page_updates: Vec<StatusUpdate>,
}

#[derive(Deserialize)]
pub struct FollowedCollectionsRequest {
    pub page_num: Option<i32>,
    pub page_size: Option<i32>,
}

#[derive(Deserialize)]
pub struct FollowedUppersRequest {
    pub page_num: Option<i32>,
    pub page_size: Option<i32>,
    pub name: Option<String>,
}

#[derive(Deserialize, Validate)]
pub struct InsertFavoriteRequest {
    pub fid: i64,
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub path: String,
}

#[derive(Deserialize, Validate)]
pub struct InsertCollectionRequest {
    pub sid: i64,
    pub mid: i64,
    #[serde(default)]
    pub collection_type: CollectionType,
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub path: String,
}

#[derive(Deserialize, Validate)]
pub struct InsertSubmissionRequest {
    pub upper_id: i64,
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub path: String,
}

#[derive(Deserialize, Validate)]
#[serde(rename_all = "camelCase")]
pub struct UpdateVideoSourceRequest {
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub path: String,
    pub enabled: bool,
    pub rule: Option<Rule>,
    pub filter_option: Option<FilterOption>,
    pub use_dynamic_api: Option<bool>,
}

#[derive(Serialize, Deserialize)]
pub struct DefaultPathRequest {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct PollQrcodeRequest {
    pub qrcode_key: String,
}

#[derive(Debug, Deserialize)]
pub struct FullSyncVideoSourceRequest {
    pub delete_local: bool,
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum OrganizationRuleType {
    Tv,
    Movie,
}

impl std::fmt::Display for OrganizationRuleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tv => write!(f, "tv"),
            Self::Movie => write!(f, "movie"),
        }
    }
}

#[derive(Deserialize, Validate)]
pub struct OrganizationRuleRequest {
    #[validate(length(min = 1, max = 100))]
    pub name: String,
    pub rule_type: OrganizationRuleType,
    /// 电视剧聚合方式：source（按源目录聚合，缺省旧行为）/ per_video（每视频一剧）；电影规则只允许 source
    pub aggregation_mode: Option<OrganizationAggregationMode>,
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub source_path: String,
    #[validate(custom(function = "crate::utils::validation::validate_path"))]
    pub output_path: String,
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationAggregationMode {
    #[default]
    Source,
    PerVideo,
}

impl std::fmt::Display for OrganizationAggregationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::PerVideo => write!(f, "per_video"),
        }
    }
}

/// 聚合方式切换请求：用户已在界面确认删除旧整理文件后重建
#[derive(Deserialize)]
pub struct OrganizationSwitchAggregationRequest {
    pub aggregation_mode: OrganizationAggregationMode,
    /// 必须为 true：显式确认删除可证明归属的旧整理文件与记录
    pub confirm_delete: bool,
    /// 上次切换未完成时的处理：retry = 继续完成目标模式切换（重新确认删除语义）；
    /// 缺省 = 仅允许全新切换，存在 pending 时拒绝
    pub pending_action: Option<OrganizationSwitchPendingAction>,
}

#[derive(Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationSwitchPendingAction {
    /// 继续完成上次未完成的切换（清理→提交→重建按目标模式续作）
    Retry,
}

impl OrganizationSwitchAggregationRequest {
    /// 未显式确认时拒绝
    pub fn ensure_confirmed(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.confirm_delete,
            "切换聚合方式会删除旧整理文件与记录后重建，需在界面确认后显式传 confirm_delete: true"
        );
        Ok(())
    }
}

#[derive(Deserialize, Default)]
pub struct OrganizationActionRequest {
    /// 电影规则必填：待整理的视频 id 列表
    pub video_ids: Option<Vec<i32>>,
}

#[derive(Debug, Deserialize)]
pub struct BrowseDirectoryRequest {
    /// 待浏览的绝对路径；缺省时从根目录开始
    pub path: Option<String>,
}
