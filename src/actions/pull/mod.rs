pub mod pullers;
use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use colored::Colorize;
use futures::TryStreamExt;
use sqlx::PgPool;

use crate::{
    actions::Action,
    config_file_manager::{ddl_config::get_uncommented_file_contents, user_config::UserConfig},
    db_manager,
};

use self::pullers::{
    data_type_puller::DataTypePuller, function_puller::FunctionPuller,
    table_data_puller::TableDataPuller, table_ddl_puller::TableDDLPuller, view_puller::ViewPuller,
};

use super::init::SCHEMA_CONFIG_LOCATION;

#[derive(sqlx::FromRow, Default, Debug)]
pub struct DDL {
    pub name: String,
    pub definition: String,
    pub file_path: String,
}

#[derive(Debug, Args)]
pub struct Pull {
    /// Pull the specified function or functions that start with the input pattern.
    #[arg(short,long, num_args(0..))]
    functions: Option<Vec<String>>,

    /// Pull the specified table ddl that starts with the input pattern.
    #[arg(short,long, num_args(0..))]
    table_ddl: Option<Vec<String>>,

    /// Pull the specified table data that starts with the input pattern.
    #[arg(short='T',long, num_args(0..))]
    table_data: Option<Vec<String>>,

    /// Pull the specified data types (enums, domains or composite types) that starts with the input pattern.
    #[arg(short,long, num_args(0..))]
    data_types: Option<Vec<String>>,

    /// Pull the specified views (materialized views and normal views) that starts with the input pattern.     
    #[arg(short,long, num_args(0..))]
    views: Option<Vec<String>>,

    /// Pull all of the DDL within the schemas that are uncommented in the schema config file found
    /// at ./.tusk/config/schemas_to_include.conf
    #[arg(short, long)]
    all: bool,
}

impl Pull {
    async fn pull_all<T: pullers::SQLPuller>(
        pool: &PgPool,
        schema_name: &str,
        config_file_path: &str,
    ) -> Result<()> {
        let mut all_ddl = T::get_all(pool, schema_name, config_file_path)?;

        while let Some(ddl) = all_ddl.try_next().await? {
            // TODO: Make this async as well.
            let file_path = format!("./schemas/{}/{}.sql", schema_name, ddl.file_path);
            if ddl.definition.is_empty() {
                println!(
                    "\t{} ({}): {}",
                    "Warning".yellow(),
                    file_path,
                    "Does not exist within the database"
                );
                continue;
            }
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

            println!("\tPulling {}", file_path.magenta());
            std::fs::write(file_path, ddl.definition)?;
        }

        return Ok(());
    }
    async fn pull_some<T: pullers::SQLPuller>(
        pool: &PgPool,
        schema_name: &str,
        config_file_path: &str,
        items: &Vec<String>,
    ) -> Result<()> {
        let mut all_ddl = T::get(pool, schema_name, config_file_path, items)?;

        while let Some(ddl) = all_ddl.try_next().await? {
            // TODO: Make this async as well.
            let file_path = format!("./schemas/{}/{}.sql", schema_name, ddl.file_path);
            if ddl.definition.is_empty() {
                println!(
                    "\t{} ({}): {}",
                    "Warning".yellow(),
                    file_path,
                    "Does not exist within the database"
                );
                continue;
            }
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

            println!("\tPulling {}", file_path.magenta());
            std::fs::write(file_path, ddl.definition)?;
        }

        return Ok(());
    }

    async fn pull_sql<T: pullers::SQLPuller>(
        &self,
        pool: &PgPool,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        input_items: &Option<Vec<String>>,
        clean_before_pull: bool,
    ) -> Result<()> {
        if self.all {
            // we want to pull everything if self.all is true

            if clean_before_pull {
                // Remove the directory before repopulating
                Self::clean_ddl_dir(ddl_parent_dir)?;
            }

            Self::pull_all::<T>(pool, schema_name, config_file_path).await?;
        }

        if let Some(input_items) = input_items {
            if input_items.len() == 0 {
                // Run a pull on all of the items

                if clean_before_pull {
                    // Remove the directory before repopulating
                    Self::clean_ddl_dir(ddl_parent_dir)?;
                }

                Self::pull_all::<T>(pool, schema_name, config_file_path).await?;
            } else {
                // Run a pull on the items in input_items
                Self::pull_some::<T>(pool, schema_name, config_file_path, input_items).await?;
            }
        }
        return Ok(());
    }

    async fn pull_pg_dump<T: pullers::PgDumpPuller>(
        &self,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        pg_bin_path: &str,
        input_items: &Option<Vec<String>>,
        clean_before_pull: bool,
    ) -> Result<()> {
        if self.all {
            // we want to pull everything if self.all is true

            if clean_before_pull {
                // Remove the directory before repopulating
                Self::clean_ddl_dir(ddl_parent_dir)?;
            }

            T::get_all(
                schema_name,
                config_file_path,
                ddl_parent_dir,
                connection_string,
                pg_bin_path,
            )
            .await?;
        }

        if let Some(input_items) = input_items {
            if input_items.len() == 0 {
                // Run a pull on all of the items

                if clean_before_pull {
                    // Remove the directory before repopulating
                    Self::clean_ddl_dir(ddl_parent_dir)?;
                }

                T::get_all(
                    schema_name,
                    config_file_path,
                    ddl_parent_dir,
                    connection_string,
                    pg_bin_path,
                )
                .await?;
            } else {
                // Run a pull on the items in input_items
                T::get(
                    schema_name,
                    config_file_path,
                    ddl_parent_dir,
                    connection_string,
                    pg_bin_path,
                    input_items,
                )
                .await?;
            }
        }
        return Ok(());
    }

    fn clean_ddl_dir(dir_path: &str) -> Result<()> {
        if std::path::Path::new(&dir_path).exists() {
            println!("\t{}: Directory {}", "Removed".yellow(), dir_path.magenta());
            std::fs::remove_dir_all(dir_path)?;
        }

        return Ok(());
    }

    async fn create_schema_def(schema: &str) -> Result<()> {
        
        let schema_ddl_dir = format!("./schemas/{}/{}.sql", schema, schema);
        let parent_path = std::path::Path::new(&schema_ddl_dir).parent().expect("There is always a parent to the above path");
        if !parent_path.exists() {
            tokio::fs::create_dir_all(&parent_path).await?;
        }

        let ddl = format!("CREATE SCHEMA IF NOT EXISTS {};\n", schema);
        tokio::fs::write(&schema_ddl_dir, ddl).await?;

        println!("\tPulling {}", schema_ddl_dir.magenta());

        return Ok(());
    }
}

#[async_trait]
impl Action for Pull {
    async fn execute(&self) -> anyhow::Result<()> {
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        let connection_string = connection.get_connection_string();
        let pg_bin_path = connection.get_pg_bin_path();

        println!("\nBeginning Pulling:");

        let clean_before_pull = UserConfig::get_global()?
            .pull_options
            .clean_ddl_before_pulling;

        for schema in approved_schemas {
            println!("\nBeginning {} schema pull:", schema);

            if self.all {
                // If we are syncing everything then also create the schema ddl file
                Self::create_schema_def(&schema).await?;
            }

            // get the function ddl
            self.pull_sql::<FunctionPuller>(
                pool,
                &schema,
                &format!(
                    "./.tusk/config/schemas/{}/functions_to_include.conf",
                    schema
                ),
                &format!("./schemas/{}/functions", schema),
                &self.functions,
                clean_before_pull,
            )
            .await?;

            // get the table ddl
            self.pull_pg_dump::<TableDDLPuller>(
                &schema,
                &format!(
                    "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
                    schema,
                ),
                &format!("./schemas/{}/table_ddl", schema),
                connection_string,
                pg_bin_path,
                &self.table_ddl,
                clean_before_pull,
            )
            .await?;

            // get the table data
            self.pull_pg_dump::<TableDataPuller>(
                &schema,
                &format!(
                    "./.tusk/config/schemas/{}/table_data_to_include.conf",
                    schema,
                ),
                &format!("./schemas/{}/table_data", schema),
                connection_string,
                pg_bin_path,
                &self.table_data,
                clean_before_pull,
            )
            .await?;

            // get the data_types ddl
            self.pull_sql::<DataTypePuller>(
                pool,
                &schema,
                &format!(
                    "./.tusk/config/schemas/{}/data_types_to_include.conf",
                    schema,
                ),
                &format!("./schemas/{}/data_types", schema),
                &self.data_types,
                clean_before_pull,
            )
            .await?;

            // get the view ddl
            self.pull_pg_dump::<ViewPuller>(
                &schema,
                &format!("./.tusk/config/schemas/{}/views_to_include.conf", schema,),
                &format!("./schemas/{}/views", schema),
                connection_string,
                pg_bin_path,
                &self.views,
                clean_before_pull,
            )
            .await?;
        }
        return Ok(());
    }
}
