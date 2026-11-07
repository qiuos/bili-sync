use std::collections::VecDeque;
use std::future::Future;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bili_sync_entity::video;
use parking_lot::RwLock;
use sea_orm::{ActiveValue::Set, DatabaseConnection, EntityTrait};
use serde_json::Value;
use tower::ServiceExt;

use crate::api::{LogHelper, routes};
use crate::bilibili::BiliClient;
use crate::config::VersionedConfig;
use crate::database::setup_database;

/// 构造一个挂好数据库与认证配置的测试路由，接线方式与 http_server 相同
///
/// 每个测试使用独立的 sqlite 文件，避免并行运行时互相删除/加锁同一数据库；
/// 全局配置为进程级 OnceCell，多个测试并行时仅首个完成的初始化生效，
/// 但各测试都写入相同的默认配置，因此复用首个配置语义一致；token 读取自全局配置。
async fn test_app(tag: &str) -> Result<(axum::Router, DatabaseConnection, TempDb)> {
    let db_path = TempDb::new(tag);
    let db = setup_database(db_path.path()).await?;
    // 测试数据库无配置记录，显式写入默认配置后再初始化全局单例（并行安全：OnceCell 幂等）
    crate::config::Config::default().save_to_database(&db).await?;
    VersionedConfig::init_for_test(&db).await?;
    let log_writer = LogHelper::new(
        tokio::sync::broadcast::channel(1).0,
        Arc::new(RwLock::new(VecDeque::new())),
    );
    let app = routes::router()
        .layer(axum::Extension(db.clone()))
        .layer(axum::Extension(Arc::new(BiliClient::new())))
        .layer(axum::Extension(log_writer));
    Ok((app, db, db_path))
}

/// 测试用 sqlite 文件的拥有者：drop 时连同 wal/shm 一起删除，不留未登记的临时文件
struct TempDb(std::path::PathBuf);

impl TempDb {
    fn new(tag: &str) -> Self {
        let mut path = std::env::temp_dir();
        path.push(format!("bili-sync-dashboard-test-{tag}-{}", std::process::id()));
        TempDb(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-shm", "-wal"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
        }
    }
}

/// 插入满足表结构 NOT NULL 约束的最小视频记录（schema 无默认值的列需显式赋值）
async fn insert_video(db: &DatabaseConnection, name: &str, created_at: &str) -> Result<()> {
    // ponytail: 仅解析一次时间字符串复用到三个时间列；如需不同时态再拆开
    let ts = chrono::NaiveDateTime::parse_from_str("2026-01-01 10:00:00", "%Y-%m-%d %H:%M:%S")?;
    video::Entity::insert(video::ActiveModel {
        upper_id: Set(1),
        upper_name: Set("UP".to_string()),
        upper_face: Set(String::new()),
        name: Set(name.to_string()),
        path: Set(format!("/tmp/{name}")),
        category: Set(0),
        bvid: Set(format!("BV-test-{name}")),
        intro: Set(String::new()),
        cover: Set(String::new()),
        ctime: Set(ts),
        pubtime: Set(ts),
        favtime: Set(ts),
        download_status: Set(0),
        valid: Set(true),
        should_download: Set(true),
        created_at: Set(created_at.to_string()),
        ..Default::default()
    })
    .exec(db)
    .await?;
    Ok(())
}

fn dashboard_json(response: axum::response::Response) -> impl Future<Output = Result<Value>> {
    Box::pin(async move {
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        Ok(serde_json::from_slice::<Value>(&bytes)?)
    })
}

/// 最近入库接口：应按入库时间（created_at）倒序返回最多 5 条记录
#[tokio::test]
async fn test_recent_videos_order_and_limit() -> Result<()> {
    let (app, db, _db_file) = test_app("order_limit").await?;
    // created_at 从旧到新插入 6 条，接口应返回最新 5 条且按时间倒序
    for i in 0..6 {
        insert_video(&db, &format!("视频{i}"), &format!("2026-01-0{} 10:00:00", i + 1)).await?;
    }
    let token = crate::config::VersionedConfig::get().read().auth_token.clone();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/dashboard")
                .header("Authorization", token)
                .body(Body::empty())?,
        )
        .await?;
    let value = dashboard_json(response).await?;
    let videos = value["data"]["recent_videos"].as_array().expect("recent_videos");
    assert_eq!(videos.len(), 5, "最多返回 5 条记录");
    // 最新入库在前：视频 6（未返回，超出限额）→ 视频 5 ... 视频 2
    assert_eq!(videos[0]["name"], "视频5");
    assert_eq!(videos[4]["name"], "视频1");
    // 字段完整性：标题、下载状态五元组、入库时间
    assert!(videos[0]["download_status"].is_array());
    assert_eq!(videos[0]["download_status"].as_array().unwrap().len(), 5);
    assert!(videos[0]["created_at"].is_string());
    Ok(())
}

/// 无视频时返回空列表，而非报错
#[tokio::test]
async fn test_recent_videos_empty() -> Result<()> {
    let (app, _db, _db_file) = test_app("empty").await?;
    let token = crate::config::VersionedConfig::get().read().auth_token.clone();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/dashboard")
                .header("Authorization", token)
                .body(Body::empty())?,
        )
        .await?;
    let value = dashboard_json(response).await?;
    assert!(value["data"]["recent_videos"].as_array().unwrap().is_empty());
    Ok(())
}
