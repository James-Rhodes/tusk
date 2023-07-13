pub mod pullers;
use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;
use futures::TryStreamExt;
use sqlx::PgPool;
use walkdir::WalkDir;

use crate::{
    config_file_manager::{
        ddl_config::{
            format_config_file, get_matching_file_contents, get_uncommented_file_contents,
        },
        user_config::UserConfig,
    },
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

    /// Adding this flag will give a preview of what is going to be pulled and allow the user to accept or
    /// deny the items to be pulled.
    #[arg(long)]
    confirm: bool,

    #[clap(skip)]
    pg_bin_path: String,

    #[clap(skip)]
    connection_string: String,

    #[clap(skip)]
    clean_before_pull: bool,

    #[clap(skip)]
    user_config_confirm_before_pull: bool,
}

impl Pull {
    async fn pull_sql<T: pullers::SQLPuller>(
        &self,
        pool: &PgPool,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        input_items: &Option<Vec<String>>,
    ) -> Result<()> {
        let items_to_pull =
            self.get_items_to_pull(schema_name, config_file_path, ddl_parent_dir, input_items)?;

        if let Some(items_to_pull) = items_to_pull {
            let mut all_ddl = T::get(pool, schema_name, &items_to_pull)?;

            while let Some(ddl) = all_ddl.try_next().await? {
                let file_path = format!("./schemas/{}/{}.sql", schema_name, ddl.file_path);
                if ddl.definition.is_empty() {
                    println!(
                        "\t{} ({}): Does not exist within the database",
                        "Warning".yellow(),
                        file_path
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
        }

        Ok(())
    }

    async fn pull_pg_dump<T: pullers::PgDumpPuller>(
        &self,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        input_items: &Option<Vec<String>>,
    ) -> Result<()> {
        let items_to_pull =
            self.get_items_to_pull(schema_name, config_file_path, ddl_parent_dir, input_items)?;

        if let Some(items_to_pull) = items_to_pull {
            T::get(
                schema_name,
                ddl_parent_dir,
                &self.connection_string,
                &self.pg_bin_path,
                &items_to_pull,
            )
            .await?;
        }
        Ok(())
    }

    fn clean_ddl_dir(dir_path: &str) -> Result<()> {
        if !std::path::Path::new(&dir_path).exists() {
            // Path doesn't exist so just return
            return Ok(());
        }

        if dir_path.ends_with("functions") {
            // This is a function directory and so we need extra logic to ensure unit tests aren't
            // removed
            Self::clean_function_dir(dir_path)?;
        } else {
            std::fs::remove_dir_all(dir_path)?;
        }

        println!("\t{}: Directory {}", "Cleaned".yellow(), dir_path.magenta());
        Ok(())
    }

    fn clean_function_dir(dir_path: &str) -> Result<()> {
        for entry in WalkDir::new(dir_path)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_dir())
        {
            let mut unit_test_path = entry.into_path();
            unit_test_path.push(std::path::Path::new("unit_tests"));

            if !unit_test_path.exists() {
                // Don't delete the directory if unit tests are defined within the directory
                std::fs::remove_dir_all(
                    unit_test_path
                        .parent()
                        .context("This path must have a parent")?,
                )?;
            }
        }

        Ok(())
    }

    fn get_items_to_pull(
        &self,
        schema_name: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        input_items: &Option<Vec<String>>,
    ) -> Result<Option<Vec<String>>> {
        format_config_file(config_file_path)?;

        match (self.all, input_items) {
            (false, None) => Ok(None),
            (true, _) => {
                // Get all items
                let items = get_uncommented_file_contents(config_file_path)?;

                if (self.user_config_confirm_before_pull || self.confirm)
                    && !items.is_empty()
                    && !UserConfig::user_confirmed(&items)?
                {
                    anyhow::bail!("The items were rejected by the user. Please filter appropriately on the next run")
                }

                if self.clean_before_pull {
                    // Remove the directory before repopulating
                    Self::clean_ddl_dir(ddl_parent_dir)?;
                }

                Ok(Some(items))
            }
            (false, Some(items)) if items.is_empty() => {
                // Get all items
                let items = get_uncommented_file_contents(config_file_path)?;

                if (self.user_config_confirm_before_pull || self.confirm)
                    && !items.is_empty()
                    && !UserConfig::user_confirmed(&items)?
                {
                    anyhow::bail!("The items were rejected by the user. Please filter appropriately on the next run")
                }

                if self.clean_before_pull {
                    // Remove the directory before repopulating
                    Self::clean_ddl_dir(ddl_parent_dir)?;
                }

                Ok(Some(items))
            }
            (false, Some(items)) if !items.is_empty() => {
                // The user specified some patterns to
                // match
                let approved_items = get_uncommented_file_contents(config_file_path)?;
                let items =
                    get_matching_file_contents(approved_items.iter(), &items, Some(schema_name))?
                        .into_iter()
                        .cloned()
                        .collect::<Vec<String>>();

                if (self.user_config_confirm_before_pull || self.confirm)
                    && !items.is_empty()
                    && !UserConfig::user_confirmed(&items)?
                {
                    anyhow::bail!("The items were rejected by the user. Please filter appropriately on the next run")
                }

                Ok(Some(items))
            }
            _ => unreachable!(),
        }
    }

    async fn create_schema_def(schema: &str) -> Result<()> {
        let schema_ddl_dir = format!("./schemas/{}/{}.sql", schema, schema);
        let parent_path = std::path::Path::new(&schema_ddl_dir)
            .parent()
            .expect("There is always a parent to the above path");
        if !parent_path.exists() {
            tokio::fs::create_dir_all(&parent_path).await?;
        }

        let ddl = format!("CREATE SCHEMA IF NOT EXISTS {};\n", schema);
        tokio::fs::write(&schema_ddl_dir, ddl).await?;

        println!("\tPulling {}", schema_ddl_dir.magenta());

        Ok(())
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        self.connection_string = connection.get_connection_string().to_owned();

        self.pg_bin_path = connection.get_pg_bin_path().to_owned();

        self.clean_before_pull = UserConfig::get_global()?
            .pull_options
            .clean_ddl_before_pulling;

        self.user_config_confirm_before_pull =
            UserConfig::get_global()?.pull_options.confirm_before_pull;

        println!("\nBeginning Pulling:");

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
                &self.table_ddl,
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
                &self.table_data,
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
            )
            .await?;

            // get the view ddl
            self.pull_pg_dump::<ViewPuller>(
                &schema,
                &format!("./.tusk/config/schemas/{}/views_to_include.conf", schema,),
                &format!("./schemas/{}/views", schema),
                &self.views,
            )
            .await?;
        }
        Ok(())
    }
}
