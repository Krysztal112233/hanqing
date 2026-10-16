pub use sea_orm_migration::prelude::*;

mod m20261011_124349_create_messages_table;
mod m20261011_130243_create_session_tables;
mod m20261011_143116_create_sync_state_table;
mod m20261011_144544_add_messages_pgroonga_index;
mod m20261011_150904_create_media_table;
mod m20261011_165847_archive_controls;
mod m20261011_180030_message_versions;
mod m20261011_180030_split_session_update_state;
mod m20261015_031410_create_media_queue;
mod m20261016_054634_create_peers_table;
mod m20261016_120122_create_backfill_queue;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20261011_124349_create_messages_table::Migration),
            Box::new(m20261011_130243_create_session_tables::Migration),
            Box::new(m20261011_143116_create_sync_state_table::Migration),
            Box::new(m20261011_144544_add_messages_pgroonga_index::Migration),
            Box::new(m20261011_150904_create_media_table::Migration),
            Box::new(m20261011_165847_archive_controls::Migration),
            Box::new(m20261011_180030_message_versions::Migration),
            Box::new(m20261011_180030_split_session_update_state::Migration),
            Box::new(m20261015_031410_create_media_queue::Migration),
            Box::new(m20261016_054634_create_peers_table::Migration),
            Box::new(m20261016_120122_create_backfill_queue::Migration),
        ]
    }
}
