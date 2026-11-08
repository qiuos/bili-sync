//! 单视频重新下载复用正常下载流程；调用方必须持有下载任务锁。
use anyhow::{Context, Result, bail};
use bili_sync_entity::*;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, TransactionTrait};

use crate::adapter::{VideoSource, VideoSourceEnum};
use crate::bilibili::{self, BiliClient};
use crate::config::{TEMPLATE, VersionedConfig};
use crate::downloader::Downloader;
use crate::utils::download_context::DownloadContext;
use crate::utils::model::update_videos_model;
use crate::workflow::download_video_pages;

pub async fn run(video_id: i32, db: &DatabaseConnection, client: &BiliClient) -> Result<()> {
    let video = video::Entity::find_by_id(video_id)
        .one(db)
        .await?
        .context("视频不存在")?;
    let source: VideoSourceEnum = if let Some(id) = video.favorite_id {
        favorite::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("视频源不存在")?
            .into()
    } else if let Some(id) = video.collection_id {
        collection::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("视频源不存在")?
            .into()
    } else if let Some(id) = video.submission_id {
        submission::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("视频源不存在")?
            .into()
    } else if let Some(id) = video.watch_later_id {
        watch_later::Entity::find_by_id(id)
            .one(db)
            .await?
            .context("视频源不存在")?
            .into()
    } else {
        bail!("视频没有关联的视频源");
    };
    let pages = page::Entity::find()
        .filter(page::Column::VideoId.eq(video_id))
        .all(db)
        .await?;
    if video.single_page.is_none() || pages.is_empty() {
        bail!("视频详情尚未填充，无法重新下载");
    }
    let config = VersionedConfig::get().snapshot();
    config.check()?;
    let mixin = client
        .wbi_img(&config.credential)
        .await?
        .into_mixin_key()
        .context("解析 mixin key 失败")?;
    bilibili::set_global_mixin_key(mixin);
    let client = client.snapshot()?;
    let template = TEMPLATE.snapshot();
    let downloader = Downloader::new(client.client.clone());
    let source_filter = source
        .filter_option()
        .as_ref()
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()?;
    let filter = source_filter.as_ref().unwrap_or(&config.filter_option);
    let cx = DownloadContext::new(&client, &source, &template, db, &downloader, &config, filter);

    let txn = db.begin().await?;
    video::Entity::update_many()
        .col_expr(video::Column::DownloadStatus, Expr::value(0))
        .filter(video::Column::Id.eq(video_id))
        .exec(&txn)
        .await?;
    page::Entity::update_many()
        .col_expr(page::Column::DownloadStatus, Expr::value(0))
        .filter(page::Column::VideoId.eq(video_id))
        .exec(&txn)
        .await?;
    txn.commit().await?;
    let mut video = video;
    video.download_status = 0;
    let pages = pages
        .into_iter()
        .map(|mut page| {
            page.download_status = 0;
            page
        })
        .collect();
    let upper_uids = video.uppers().map(|upper| upper.mid).collect();
    let model = download_video_pages(video, pages, upper_uids, cx).await?;
    let status: [u32; 5] =
        crate::utils::status::VideoStatus::from(*model.download_status.try_as_ref().context("缺少下载状态")?).into();
    update_videos_model(vec![model], db).await?;
    if status.iter().any(|value| *value != crate::utils::status::STATUS_OK) {
        bail!("重新下载未完成，请查看各子任务日志");
    }
    crate::workflow::organize_completed_video(video_id, db).await;
    info!("视频 {video_id} 重新下载完成");
    Ok(())
}
