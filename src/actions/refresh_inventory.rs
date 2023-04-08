use std::collections::HashSet;

use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::actions::{init::SCHEMA_CONFIG_LOCATION, Action};
use crate::{config_file_manager, db_manager};

enum SchemaListStatus {
    FirstLoad,
    AlreadyLoaded,
}

#[derive(Debug, Args)]
pub struct RefreshInventory {}

impl RefreshInventory {
    async fn refresh_schema_list(&self, pool: &PgPool) -> Result<SchemaListStatus> {
        let db_schema_list: HashSet<String> = sqlx::query(
            "
            SELECT nspname schema_name
            FROM pg_catalog.pg_namespace
            WHERE nspname NOT ILIKE 'pg_%' AND nspname != 'information_schema'
            ORDER BY schema_name;
            ",
        )
        .map(|row: PgRow| {
            let schema_name: String = row
                .try_get("schema_name")
                .expect("The database you are pointing to should always have atleast one schema.");
            return schema_name;
        })
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();

        let change_status = config_file_manager::update_file_contents_from_db(
            SCHEMA_CONFIG_LOCATION,
            db_schema_list,
            true
        )?;

        // There was nothing in the schema config file to begin with and so it is the first load.
        if change_status.amount_before_change == 0 {
            return Ok(SchemaListStatus::FirstLoad);
        }

        println!(
            "Schema config file refreshed! Added: {}, Removed: {}",
            change_status.added, change_status.removed
        );

        return Ok(SchemaListStatus::AlreadyLoaded);
    }
}

#[async_trait]
impl Action for RefreshInventory {
    async fn execute(&self) -> Result<()> {
        let pool = db_manager::get_db_connection().await?;
        if let SchemaListStatus::FirstLoad = self.refresh_schema_list(&pool).await? {
            println!("\n\nThe list of schemas has been initialised at {}\n\nPlease comment out using // any schemas you do not wish to back up before running refresh-inventory again. This will create the lists of functions and tables for you to configure", std::env::current_dir().unwrap().to_str().unwrap().to_owned() + &SCHEMA_CONFIG_LOCATION[1..]);

            return Ok(());
        }
        return Ok(());
    }
}
