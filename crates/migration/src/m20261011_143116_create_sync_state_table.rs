use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Backfill watermark per chat: `cursor` is the oldest msg_id fetched so
        // far (next page continues below it); NULL cursor = not started.
        manager
            .create_table(
                Table::create()
                    .table(SyncState::Table)
                    .if_not_exists()
                    .col(big_integer(SyncState::ChatId).not_null().primary_key())
                    .col(big_integer(SyncState::Cursor).null())
                    .col(boolean(SyncState::Done).not_null().default(false))
                    .col(
                        timestamp_with_time_zone(SyncState::UpdatedAt)
                            .not_null()
                            .default(Expr::current_timestamp()),
                    )
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(SyncState::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum SyncState {
    Table,
    ChatId,
    Cursor,
    Done,
    UpdatedAt,
}
