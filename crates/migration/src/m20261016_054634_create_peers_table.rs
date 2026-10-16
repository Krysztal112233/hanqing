use sea_orm_migration::sea_query::extension::postgres::Type;
use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_type(
                Type::create()
                    .as_enum(PeerKind::Enum)
                    .values([
                        PeerKind::User,
                        PeerKind::Group,
                        PeerKind::Channel,
                        PeerKind::Community,
                    ])
                    .to_owned(),
            )
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(Peers::Table)
                    .col(big_integer(Peers::Id).primary_key())
                    .col(
                        enumeration(
                            Peers::Kind,
                            PeerKind::Enum,
                            [
                                PeerKind::User,
                                PeerKind::Group,
                                PeerKind::Channel,
                                PeerKind::Community,
                            ],
                        )
                        .not_null(),
                    )
                    .col(text_null(Peers::Name))
                    .col(text_null(Peers::Username))
                    .col(
                        timestamp_with_time_zone(Peers::UpdatedAt)
                            .not_null()
                            .default(Expr::cust("now()")),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Peers::Table).to_owned())
            .await?;
        manager
            .drop_type(Type::drop().name(PeerKind::Enum).to_owned())
            .await?;
        Ok(())
    }
}

/// Display-name cache for Telegram peers (users/groups/channels). `id` uses
/// the bot-API dialog-id convention, so it joins directly against
/// messages.chat_id and messages.sender_id.
#[derive(DeriveIden)]
enum Peers {
    Table,
    Id,
    Kind,
    Name,
    Username,
    UpdatedAt,
}

#[derive(DeriveIden)]
enum PeerKind {
    #[sea_orm(iden = "peer_kind")]
    Enum,
    #[sea_orm(iden = "user")]
    User,
    #[sea_orm(iden = "group")]
    Group,
    #[sea_orm(iden = "channel")]
    Channel,
    #[sea_orm(iden = "community")]
    Community,
}
