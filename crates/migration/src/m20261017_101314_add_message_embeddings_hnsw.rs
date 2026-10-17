use sea_orm_migration::prelude::*;

pub struct Migration;

impl MigrationName for Migration {
    fn name(&self) -> &str {
        "m20261017_101314_add_message_embeddings_hnsw"
    }
}

/// HNSW expression index used by `search_neighbors`:
/// `(embedding::halfvec(1024)) halfvec_cosine_ops`.
fn hnsw_index() -> IndexCreateStatement {
    Index::create()
        .name("message_embeddings_hnsw")
        .table(Alias::new("message_embeddings"))
        .col(
            IndexColumn::from(Expr::cust("embedding::halfvec(1024)"))
                .with_operator_class(Alias::new("halfvec_cosine_ops")),
        )
        .index_type(IndexType::Custom(Alias::new("hnsw").into_iden()))
        .if_not_exists()
        .to_owned()
}

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Non-concurrent is fine here because migrations run before the
        // daemon starts; this is a no-op if the index was already built
        // online.
        manager.create_index(hnsw_index()).await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("message_embeddings_hnsw")
                    .if_exists()
                    .to_owned(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hnsw_index_uses_expression_and_operator_class() {
        assert_eq!(
            hnsw_index().to_string(PostgresQueryBuilder),
            r#"CREATE INDEX IF NOT EXISTS "message_embeddings_hnsw" ON "message_embeddings" USING hnsw ((embedding::halfvec(1024)) halfvec_cosine_ops)"#
        );
    }

    #[test]
    fn drop_index_uses_builder() {
        let sql = Index::drop()
            .name("message_embeddings_hnsw")
            .if_exists()
            .to_owned()
            .to_string(PostgresQueryBuilder);
        assert_eq!(sql, r#"DROP INDEX IF EXISTS "message_embeddings_hnsw""#);
    }
}
