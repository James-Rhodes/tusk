use anyhow::Result;
use futures::TryStreamExt;
use sqlx::Row;
use clap::Args;
use async_trait::async_trait;

use crate::actions::Action;
use crate::db_manager;

#[derive(Debug, Args)]
pub struct RefreshInventory {}

impl RefreshInventory {
    async fn test(&self) -> Result<()> {
        let pool = db_manager::get_db_connection().await?;
        let mut rows = sqlx::query("SELECT * FROM test_table").fetch(&pool);

        while let Some(row) = rows.try_next().await? {
            let value: i32 = row.try_get("value")?;
            let text: String = row.try_get("some_text")?;

            println!("value: {:?}, text: {:?}", value, text);
        }

        return Ok(());
    }
}


#[async_trait]
impl Action for RefreshInventory {
    async fn execute(&self) -> anyhow::Result<()> {
        self.test().await?;
        return Ok(());
    }
}
