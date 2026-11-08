use bili_sync_entity::rule::Rule;
use bili_sync_entity::*;
use sea_orm::prelude::DateTime;
use sea_orm::{DerivePartialModel, FromQueryResult};
use serde::Serialize;

use crate::bilibili::{PollStatus, Qrcode};
use crate::utils::status::{PageStatus, VideoStatus};

#[derive(Serialize)]
pub struct VideoSourcesResponse {
    pub collection: Vec<VideoSource>,
    pub favorite: Vec<VideoSource>,
    pub submission: Vec<VideoSource>,
    pub watch_later: Vec<VideoSource>,
}

#[derive(Serialize)]
pub struct VideosResponse {
    pub videos: Vec<VideoInfo>,
    pub total_count: u64,
}

#[derive(Serialize)]
pub struct VideoResponse {
    pub video: VideoInfo,
    pub pages: Vec<PageInfo>,
}

#[derive(Serialize)]
pub struct ResetVideoResponse {
    pub resetted: bool,
    pub video: VideoInfo,
    pub pages: Vec<PageInfo>,
}

#[derive(Serialize)]
pub struct ResetFilteredVideosResponse {
    pub resetted: bool,
    pub resetted_videos_count: usize,
    pub resetted_pages_count: usize,
}

#[derive(Serialize)]
pub struct UpdateVideoStatusResponse {
    pub success: bool,
    pub video: VideoInfo,
    pub pages: Vec<PageInfo>,
}

#[derive(Serialize)]
pub struct UpdateFilteredVideoStatusResponse {
    pub success: bool,
    pub updated_videos_count: usize,
    pub updated_pages_count: usize,
}

#[derive(FromQueryResult, Serialize)]
pub struct VideoSource {
    pub id: i32,
    pub name: String,
}

#[derive(Serialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "video::Entity")]
pub struct VideoInfo {
    pub id: i32,
    pub bvid: String,
    pub name: String,
    pub upper_name: String,
    pub valid: bool,
    pub should_download: bool,
    #[serde(serialize_with = "serde_video_download_status")]
    pub download_status: u32,
    pub collection_id: Option<i32>,
    pub favorite_id: Option<i32>,
    pub submission_id: Option<i32>,
    pub watch_later_id: Option<i32>,
}

#[derive(Serialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "page::Entity")]
pub struct PageInfo {
    pub id: i32,
    pub video_id: i32,
    pub pid: i32,
    pub name: String,
    #[serde(serialize_with = "serde_page_download_status")]
    pub download_status: u32,
}

#[derive(Serialize, DerivePartialModel, FromQueryResult, Clone, Copy)]
#[sea_orm(entity = "video::Entity")]
pub struct SimpleVideoInfo {
    pub id: i32,
    pub download_status: u32,
}

#[derive(Serialize, DerivePartialModel, FromQueryResult, Clone, Copy)]
#[sea_orm(entity = "page::Entity")]
pub struct SimplePageInfo {
    pub id: i32,
    pub video_id: i32,
    pub download_status: u32,
}

fn serde_video_download_status<S>(status: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let status: [u32; 5] = VideoStatus::from(*status).into();
    status.serialize(serializer)
}

fn serde_page_download_status<S>(status: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let status: [u32; 5] = PageStatus::from(*status).into();
    status.serialize(serializer)
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Followed {
    Favorite {
        title: String,
        media_count: i64,
        fid: i64,
        mid: i64,
        invalid: bool,
        subscribed: bool,
    },
    Collection {
        title: String,
        sid: i64,
        mid: i64,
        media_count: i64,
        invalid: bool,
        subscribed: bool,
    },
    Upper {
        mid: i64,
        uname: String,
        face: String,
        sign: String,
        invalid: bool,
        subscribed: bool,
    },
}

#[derive(Serialize)]
pub struct FavoritesResponse {
    pub favorites: Vec<Followed>,
}

#[derive(Serialize)]
pub struct CollectionsResponse {
    pub collections: Vec<Followed>,
    pub total: i64,
}

#[derive(Serialize)]
pub struct UppersResponse {
    pub uppers: Vec<Followed>,
    pub total: i64,
}

#[derive(Serialize)]
pub struct VideoSourcesDetailsResponse {
    pub collections: Vec<VideoSourceDetail>,
    pub favorites: Vec<VideoSourceDetail>,
    pub submissions: Vec<VideoSourceDetail>,
    pub watch_later: Vec<VideoSourceDetail>,
}

#[derive(Serialize)]
pub struct DashBoardResponse {
    pub recent_videos: Vec<RecentVideoInfo>,
}

#[derive(Serialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "video::Entity")]
pub struct RecentVideoInfo {
    pub id: i32,
    pub name: String,
    pub valid: bool,
    pub should_download: bool,
    #[serde(serialize_with = "serde_video_download_status")]
    pub download_status: u32,
    /// 数据库入库时间，即记录在数据库中新增的时间，与视频发布时间无关
    pub created_at: String,
}

#[derive(Serialize, Clone, Copy)]
pub struct SysInfo {
    pub timestamp: i64,
    pub total_memory: u64,
    pub used_memory: u64,
    pub process_memory: u64,
    pub used_cpu: f32,
    pub process_cpu: f32,
    pub total_disk: u64,
    pub available_disk: u64,
}

#[derive(Serialize, FromQueryResult)]
#[serde(rename_all = "camelCase")]
pub struct VideoSourceDetail {
    pub id: i32,
    pub name: String,
    pub path: String,
    pub rule: Option<Rule>,
    pub filter_option: Option<serde_json::Value>,
    #[serde(default)]
    pub rule_display: Option<String>,
    #[serde(default)]
    pub use_dynamic_api: Option<bool>,
    pub enabled: bool,
    pub latest_row_at: Option<DateTime>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateVideoSourceResponse {
    pub rule_display: Option<String>,
}

pub type GenerateQrcodeResponse = Qrcode;

pub type PollQrcodeResponse = PollStatus;

#[derive(Serialize)]
pub struct FullSyncVideoSourceResponse {
    pub removed_count: usize,
    pub warnings: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct OrganizationRuleResponse {
    pub id: i32,
    pub name: String,
    pub rule_type: String,
    pub source_path: String,
    pub output_path: String,
}

#[derive(Serialize)]
pub struct OrganizationRulesResponse {
    pub rules: Vec<OrganizationRuleResponse>,
}

#[derive(Serialize)]
pub struct OrganizationPlanItem {
    pub video_id: i32,
    pub page_id: i32,
    /// 跨规则去重与冲突判定的稳定标识：bvid + cid
    pub bvid: String,
    pub cid: i64,
    pub video_name: String,
    pub page_name: String,
    pub series: Option<String>,
    pub episode: Option<i32>,
    /// create / refresh / exists / conflict / missing / failed
    pub action: String,
    pub target: Option<String>,
    pub message: Option<String>,
}

#[derive(Serialize)]
pub struct OrganizationPlanResponse {
    pub items: Vec<OrganizationPlanItem>,
    pub warnings: Vec<String>,
}

#[derive(Serialize)]
pub struct OrganizationExecuteResponse {
    pub created: usize,
    pub refreshed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub extras_created: usize,
    pub items: Vec<OrganizationPlanItem>,
    pub warnings: Vec<String>,
}

#[derive(Serialize)]
pub struct DirectoryEntry {
    pub path: String,
    pub name: String,
}

#[derive(Serialize)]
pub struct DirectoryBrowseResponse {
    /// 规范化后的当前目录
    pub path: String,
    /// 父目录，根目录为 None
    pub parent: Option<String>,
    /// 当前目录下的全部子目录（按名称排序，不含文件）
    pub entries: Vec<DirectoryEntry>,
}

/// 强制重新下载请求已受理。
#[derive(Serialize)]
pub struct RedownloadVideoResponse {
    pub started: bool,
}
