pub mod syncers;
use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use futures::TryStreamExt;
use sqlx::PgPool;

use crate::{
    actions::Action,
    config_file_manager::get_uncommented_file_contents,
    db_manager::{self, get_connection_string},
};

use self::syncers::{function_syncer::FunctionSyncer, table_ddl_syncer::TableDDLSyncer};

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
    async fn sync_all<T: syncers::SQLSyncer>(pool: &PgPool, schema_name: &str) -> Result<()> {
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
    async fn sync_some<T: syncers::SQLSyncer>(
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

    async fn sync_sql<T: syncers::SQLSyncer>(
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

    fn sync_pg_dump<T: syncers::PgDumpSyncer>(
        &self,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        input_items: &Option<Vec<String>>,
    ) -> Result<()> {
        if let Some(input_items) = input_items {
            if input_items.len() == 0 {
                // Run a sync on all of the items
                T::get_all(
                    schema_name,
                    config_file_path,
                    ddl_parent_dir,
                    connection_string,
                )?;
            } else {
                // Run a sync on the items in input_items
                T::get(
                    schema_name,
                    config_file_path,
                    ddl_parent_dir,
                    connection_string,
                    input_items,
                )?;
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

        // This is getting unmanageable. Create a Sync version that works for sql queries that can
        // get the ddl. Write another version that works with another trait that defines how to
        // call pg-dump for what is required.

        let connection_string = get_connection_string()?;
        for schema in approved_schemas {
            self.sync_sql::<FunctionSyncer>(&pool, &schema, &self.function)
                .await?;
            // schema: &str, config_file_path: &str, ddl_parent_dir: &str, connection_string: &str, items: &Vec<String>
            self.sync_pg_dump::<TableDDLSyncer>(
                &schema,
                &format!(
                    "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
                    schema,
                ),
                &format!("./schemas/{}/table_ddl", schema),
                &connection_string, 
                &self.table_ddl,
            )?;
        }
        return Ok(());
    }
}
