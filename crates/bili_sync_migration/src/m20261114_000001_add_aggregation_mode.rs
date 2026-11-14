use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // 既有规则默认 source（按源目录聚合，保持旧行为）；per_video 为按视频聚合（每视频一剧）。
        // pending_aggregation_mode：进行中的聚合方式切换目标；切换完成前整理执行一律拒绝，
        // 保证中断后可重试且不会按旧模式自动重建
        if !manager.has_column("organization_rule", "aggregation_mode").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(OrganizationRule::Table)
                        .add_column(
                            ColumnDef::new(OrganizationRule::AggregationMode)
                                .string_len(16)
                                .not_null()
                                .default("source"),
                        )
                        .to_owned(),
                )
                .await?;
        }
        if !manager
            .has_column("organization_rule", "pending_aggregation_mode")
            .await?
        {
            manager
                .alter_table(
                    Table::alter()
                        .table(OrganizationRule::Table)
                        .add_column(
                            ColumnDef::new(OrganizationRule::PendingAggregationMode)
                                .string_len(16)
                                .null(),
                        )
                        .to_owned(),
                )
                .await?;
        }
        if !manager.has_column("organization_rule", "preserve_metadata").await? {
            manager
                .alter_table(
                    Table::alter()
                        .table(OrganizationRule::Table)
                        .add_column(
                            ColumnDef::new(OrganizationRule::PreserveMetadata)
                                .boolean()
                                .not_null()
                                .default(false),
                        )
                        .to_owned(),
                )
                .await?;
        }
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(OrganizationRule::Table)
                    .drop_column(OrganizationRule::PreserveMetadata)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(OrganizationRule::Table)
                    .drop_column(OrganizationRule::PendingAggregationMode)
                    .to_owned(),
            )
            .await?;
        manager
            .alter_table(
                Table::alter()
                    .table(OrganizationRule::Table)
                    .drop_column(OrganizationRule::AggregationMode)
                    .to_owned(),
            )
            .await
    }
}

#[derive(DeriveIden)]
enum OrganizationRule {
    Table,
    AggregationMode,
    PendingAggregationMode,
    PreserveMetadata,
}
