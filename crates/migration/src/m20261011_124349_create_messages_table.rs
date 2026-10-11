use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Messages::Table)
                    .if_not_exists()
                    .col(big_integer(Messages::ChatId).not_null())
                    .col(big_integer(Messages::MsgId).not_null())
                    .col(timestamp_with_time_zone(Messages::Date).not_null())
                    // NULL for channel posts and anonymous senders.
                    .col(big_integer(Messages::SenderId).null())
                    .col(text(Messages::Text).not_null().default(""))
                    // Full Telegram TL payload (externally tagged enum JSON).
                    .col(json_binary(Messages::Raw).not_null())
                    .primary_key(Index::create().col(Messages::ChatId).col(Messages::MsgId))
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Messages::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum Messages {
    Table,
    ChatId,
    MsgId,
    Date,
    SenderId,
    Text,
    Raw,
}
