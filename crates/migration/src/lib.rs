pub use sea_orm_migration::prelude::*;

mod m20261011_124349_create_messages_table;
mod m20261011_130243_create_session_tables;
mod m20261011_143116_create_sync_state_table;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20261011_124349_create_messages_table::Migration),
            Box::new(m20261011_130243_create_session_tables::Migration),
            Box::new(m20261011_143116_create_sync_state_table::Migration),
        ]
    }
}
