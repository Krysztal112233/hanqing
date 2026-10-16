use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // pgmq DDL is extension-specific; the schema builder cannot express
        // it, so raw SQL stays the convention for queue creation.
        manager
            .get_connection()
            .execute_unprepared("SELECT pgmq.create('backfill_run')")
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("SELECT pgmq.drop_queue('backfill_run')")
            .await?;
        Ok(())
    }
}
