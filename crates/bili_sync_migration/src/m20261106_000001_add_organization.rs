use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(OrganizationRule::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(OrganizationRule::Id)
                            .unsigned()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(OrganizationRule::Name).string().not_null())
                    .col(ColumnDef::new(OrganizationRule::RuleType).string().not_null())
                    .col(ColumnDef::new(OrganizationRule::SourcePath).string().not_null())
                    .col(ColumnDef::new(OrganizationRule::OutputPath).string().not_null())
                    .col(
                        ColumnDef::new(OrganizationRule::CreatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(OrganizationOutput::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(OrganizationOutput::Id)
                            .unsigned()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(OrganizationOutput::RuleId).unsigned().not_null())
                    // bvid + cid 是跨来源、跨分页重建都稳定的视频标识，去重/冲突/续编号都以它为准
                    .col(ColumnDef::new(OrganizationOutput::Bvid).string().not_null())
                    .col(ColumnDef::new(OrganizationOutput::Cid).unsigned().not_null())
                    .col(ColumnDef::new(OrganizationOutput::PageId).unsigned().not_null())
                    // 电视剧规则下记录剧名与集号，保证集号持久稳定；电影规则下为空
                    .col(ColumnDef::new(OrganizationOutput::Series).string().null())
                    .col(ColumnDef::new(OrganizationOutput::Episode).unsigned().null())
                    .col(ColumnDef::new(OrganizationOutput::Path).string().not_null())
                    .col(
                        ColumnDef::new(OrganizationOutput::CreatedAt)
                            .timestamp()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;
        manager
            .create_index(
                Index::create()
                    .name("idx_organization_output_rule_video")
                    .table(OrganizationOutput::Table)
                    .col(OrganizationOutput::RuleId)
                    .col(OrganizationOutput::Bvid)
                    .col(OrganizationOutput::Cid)
                    .unique()
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx_organization_output_rule_video")
                    .table(OrganizationOutput::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_table(Table::drop().table(OrganizationOutput::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(OrganizationRule::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum OrganizationRule {
    Table,
    Id,
    Name,
    RuleType,
    SourcePath,
    OutputPath,
    CreatedAt,
}

#[derive(DeriveIden)]
enum OrganizationOutput {
    Table,
    Id,
    RuleId,
    Bvid,
    Cid,
    PageId,
    Series,
    Episode,
    Path,
    CreatedAt,
}
