use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20261017_102143_add_enqueue_text_embed_procedure"
    }
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // One CALL drives the whole backlog backfill server-side. Batches are
        // fixed at 1000 and committed individually, so progress is durable and
        // the client does not send one statement per page.
        manager
            .get_connection()
            .execute_unprepared(
                r#"CREATE OR REPLACE PROCEDURE enqueue_text_embed()
LANGUAGE plpgsql
AS $$
DECLARE
    batch jsonb[] := ARRAY[]::jsonb[];
    n integer := 0;
    last_chat bigint := -9223372036854775808;
    last_msg bigint := -9223372036854775808;
    r record;
BEGIN
    LOOP
        batch := ARRAY[]::jsonb[];
        n := 0;

        FOR r IN
            SELECT chat_id, msg_id
            FROM messages
            WHERE deleted_at IS NULL
              AND text <> ''
              AND (chat_id, msg_id) > (last_chat, last_msg)
            ORDER BY chat_id, msg_id
            LIMIT 1000
        LOOP
            batch := array_append(
                batch,
                jsonb_build_object('chat_id', r.chat_id, 'msg_id', r.msg_id)
            );
            last_chat := r.chat_id;
            last_msg := r.msg_id;
            n := n + 1;
        END LOOP;

        EXIT WHEN n = 0;

        PERFORM pgmq.send_batch('text_embed', batch);
        COMMIT;
        RAISE NOTICE 'enqueue_text_embed: % tasks (last %,%)', n, last_chat, last_msg;
    END LOOP;
END;
$$;"#,
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP PROCEDURE IF EXISTS enqueue_text_embed()")
            .await?;
        Ok(())
    }
}
