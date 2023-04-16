pub mod syncers;
use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use futures::TryStreamExt;
use sqlx::PgPool;

use crate::{actions::Action, config_file_manager::get_uncommented_file_contents, db_manager};

use self::syncers::function_syncer::FunctionSyncer;

use super::init::SCHEMA_CONFIG_LOCATION;

#[derive(sqlx::FromRow, Default, Debug)]
pub struct DDL {
    pub name: String,
    pub definition: String,
    pub file_path: String,
}

#[derive(Debug, Args)]
pub struct Sync {
    /// Sync the specified function
    #[arg(short,long, num_args(0..))]
    function: Option<Vec<String>>,

    /// Sync the specified table ddl
    #[arg(short,long, num_args(0..))]
    table_ddl: Option<Vec<String>>,

    /// Sync the specified table data
    #[arg(short='T',long, num_args(0..))]
    table_data: Option<Vec<String>>,

    /// Sync the specified data types
    #[arg(short,long, num_args(0..))]
    data_types: Option<Vec<String>>,

    #[arg(short, long)]
    all: bool,
}

impl Sync {
    async fn sync_all<T: syncers::Syncer>(pool: &PgPool, schema_name: &str) -> Result<()> {
        let mut all_ddl = T::get_all(pool, schema_name)?;

        while let Some(ddl) = all_ddl.try_next().await? {
            // TODO: Make this async as well.
            let file_path = format!("./schemas/{}/{}.sql", schema_name, ddl.file_path);
            let parent_dir =
                std::path::Path::new(&file_path)
                    .parent()
                    .ok_or(anyhow::Error::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "The directory {} is invalid for writing files to...",
                            file_path
                        ),
                    )))?;

            if !parent_dir.exists() {
                std::fs::create_dir_all(parent_dir)?;
            }

            std::fs::write(file_path, ddl.definition)?;
        }

        return Ok(());
    }
    async fn sync_some<T: syncers::Syncer>(
        pool: &PgPool,
        schema_name: &str,
        items: &Vec<String>,
    ) -> Result<()> {

        let mut all_ddl = T::get(pool, schema_name, items)?;

        while let Some(ddl) = all_ddl.try_next().await? {
            // TODO: Make this async as well.
            let file_path = format!("./schemas/{}/{}.sql", schema_name, ddl.file_path);
            let parent_dir =
                std::path::Path::new(&file_path)
                    .parent()
                    .ok_or(anyhow::Error::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "The directory {} is invalid for writing files to...",
                            file_path
                        ),
                    )))?;

            if !parent_dir.exists() {
                std::fs::create_dir_all(parent_dir)?;
            }

            std::fs::write(file_path, ddl.definition)?;
        }

        return Ok(());
    }

    async fn sync<T: syncers::Syncer>(
        &self,
        pool: &PgPool,
        schema_name: &str,
        input_items: &Option<Vec<String>>,
    ) -> Result<()> {
        if let Some(input_items) = input_items {
            if input_items.len() == 0 {
                // Run a sync on all of the items
                Self::sync_all::<T>(pool, schema_name).await?;
            } else {
                // Run a sync on the items in input_items
                Self::sync_some::<T>(pool, schema_name, input_items).await?;
            }
        }
        return Ok(());
    }
}

#[async_trait]
impl Action for Sync {
    async fn execute(&self) -> anyhow::Result<()> {
        let pool = db_manager::get_db_connection().await?;
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        for schema in approved_schemas {
            self.sync::<FunctionSyncer>(&pool, &schema, &self.function)
                .await?;
        }
        return Ok(());
    }
}
