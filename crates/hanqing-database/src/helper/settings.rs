//! Persistence helpers for `account_settings`: one JSONB settings document
//! per account. The document schema is `hanqing_config::AccountSettings`.

use hanqing_config::AccountSettings;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{ActiveValue::Set, DatabaseConnection, EntityTrait as _};
use snafu::ResultExt;

use crate::error::{AccountMismatchSnafu, DatabaseSnafu, Error, JsonSnafu};
use crate::model::account_settings::{ActiveModel, Column};
use crate::model::prelude::AccountSettings as AccountSettingsEntity;

#[async_trait::async_trait]
pub trait AccountSettingsHelper {
    /// Load the account's settings, creating the row with defaults on first
    /// use.
    ///
    /// Single-account guard: when rows exist but none belongs to
    /// `account_id`, the session belongs to a different account than the
    /// archive — refuse rather than silently mixing archives.
    async fn load_or_init(
        db: &DatabaseConnection,
        account_id: i64,
    ) -> Result<AccountSettings, Error> {
        let rows = AccountSettingsEntity::find()
            .all(db)
            .await
            .context(DatabaseSnafu)?;

        if rows.is_empty() {
            let settings = AccountSettings::default();
            Self::save(db, account_id, &settings).await?;
            return Ok(settings);
        }

        let Some(row) = rows.iter().find(|row| row.account_id == account_id) else {
            return AccountMismatchSnafu {
                session: account_id,
                archived: rows[0].account_id,
            }
            .fail();
        };

        serde_json::from_value(row.settings.clone()).context(JsonSnafu)
    }

    /// Upsert the account's settings document.
    async fn save(
        db: &DatabaseConnection,
        account_id: i64,
        settings: &AccountSettings,
    ) -> Result<(), Error> {
        let doc = serde_json::to_value(settings).context(JsonSnafu)?;
        AccountSettingsEntity::insert(ActiveModel {
            account_id: Set(account_id),
            settings: Set(doc),
            ..Default::default()
        })
        .on_conflict(
            OnConflict::column(Column::AccountId)
                .update_column(Column::Settings)
                .value(Column::UpdatedAt, Expr::current_timestamp())
                .to_owned(),
        )
        .exec_without_returning(db)
        .await
        .context(DatabaseSnafu)?;
        Ok(())
    }
}

impl AccountSettingsHelper for AccountSettingsEntity {}
