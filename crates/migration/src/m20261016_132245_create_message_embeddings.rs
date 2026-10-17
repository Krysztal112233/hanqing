use sea_orm_migration::{prelude::*, schema::*};

#[derive(DeriveMigrationName)]
pub struct Migration;

/// Semantic-search pipeline: pgvector storage for message embeddings plus
/// the pgmq queue carrying embed work. Storage is fp32 `vector` (SeaORM
/// codegen maps it to `PgVector`); distance queries and the HNSW index
/// work on `::halfvec` casts (expression index), which keeps the index at
/// 2 bytes/dim — same pattern as fandwill.
///
/// `halfvec` requires pgvector >= 0.7; the Dockerfile installs the current
/// PGDG package, but externally managed databases must meet that floor.
///
/// The HNSW index is intentionally NOT created here: bulk-loading the
/// backlog through a live HNSW index is several times slower. A follow-up
/// migration adds it after the initial embed pass completes.
#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        // Extension-specific DDL; the schema builder cannot express it.
        db.execute_unprepared("CREATE EXTENSION IF NOT EXISTS vector")
            .await?;
        manager
            .create_table(
                Table::create()
                    .table(MessageEmbeddings::Table)
                    .col(big_integer(MessageEmbeddings::ChatId).not_null())
                    .col(big_integer(MessageEmbeddings::MsgId).not_null())
                    .col(
                        ColumnDef::new(MessageEmbeddings::Embedding)
                            .vector(Some(1024))
                            .not_null(),
                    )
                    // Which embedding model produced the vector; a model swap
                    // coexists with old rows until they are re-embedded.
                    .col(text(MessageEmbeddings::Model).not_null())
                    .col(
                        timestamp_with_time_zone(MessageEmbeddings::EmbeddedAt)
                            .not_null()
                            .default(Expr::cust("now()")),
                    )
                    .primary_key(
                        Index::create()
                            .col(MessageEmbeddings::ChatId)
                            .col(MessageEmbeddings::MsgId),
                    )
                    .to_owned(),
            )
            .await?;
        db.execute_unprepared("SELECT pgmq.create('text_embed')")
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("SELECT pgmq.drop_queue('text_embed')")
            .await?;
        manager
            .drop_table(Table::drop().table(MessageEmbeddings::Table).to_owned())
            .await?;
        // The `vector` extension is left in place, like pgmq: it may be
        // shared by other schemas and DROP EXTENSION is destructive.
        Ok(())
    }
}

#[derive(DeriveIden)]
enum MessageEmbeddings {
    #[sea_orm(iden = "message_embeddings")]
    Table,
    ChatId,
    MsgId,
    Embedding,
    Model,
    EmbeddedAt,
}
