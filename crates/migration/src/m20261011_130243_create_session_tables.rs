use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // grammers `Session` storage (PostgreSQL-backed; the project does not
        // use SQLite). Whole session values are stored as JSONB documents.
        manager
            .create_table(
                Table::create()
                    .table(SessionDcHome::Table)
                    .if_not_exists()
                    // Exactly one row holding the home datacenter id.
                    .col(integer(SessionDcHome::DcId).not_null().primary_key())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(SessionDcOption::Table)
                    .if_not_exists()
                    .col(integer(SessionDcOption::DcId).not_null().primary_key())
                    // grammers_session::types::DcOption (includes auth_key as hex).
                    .col(json_binary(SessionDcOption::Data).not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(SessionPeer::Table)
                    .if_not_exists()
                    .col(big_integer(SessionPeer::PeerId).not_null().primary_key())
                    // grammers_session::types::PeerInfo.
                    .col(json_binary(SessionPeer::Data).not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(SessionUpdateState::Table)
                    .if_not_exists()
                    // Exactly one row (id = 1) holding grammers_session::types::UpdatesState.
                    .col(
                        integer(SessionUpdateState::Id)
                            .not_null()
                            .primary_key()
                            .check(Expr::col(SessionUpdateState::Id).eq(1)),
                    )
                    .col(json_binary(SessionUpdateState::Data).not_null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(SessionUpdateState::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(SessionPeer::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(SessionDcOption::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(SessionDcHome::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum SessionDcHome {
    Table,
    DcId,
}

#[derive(DeriveIden)]
enum SessionDcOption {
    Table,
    DcId,
    Data,
}

#[derive(DeriveIden)]
enum SessionPeer {
    Table,
    PeerId,
    Data,
}

#[derive(DeriveIden)]
enum SessionUpdateState {
    Table,
    Id,
    Data,
}
