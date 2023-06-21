use std::collections::HashSet;

use anyhow::{Result, Context};
use async_trait::async_trait;
use clap::Args;
use colored::Colorize;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::actions::{init::SCHEMA_CONFIG_LOCATION, Action};
use crate::config_file_manager::ddl_config::get_uncommented_file_contents;

use crate::config_file_manager::user_config::UserConfig;
use crate::{config_file_manager, db_manager};

enum SchemaListStatus {
    FirstLoad,
    AlreadyLoaded,
}

#[derive(Debug, Args)]
pub struct Fetch {}

impl Fetch {
    async fn fetch_list(
        &self,
        pool: &PgPool,
        query: &str,
        column_name: &str,
        file_loc: &str,
        list_type: &str,
        add_new_as_commented: bool,
        delete_items_from_config: bool,
    ) -> Result<config_file_manager::ddl_config::ChangeStatus> {
        let db_list: HashSet<String> = sqlx::query(query)
            .map(|row: PgRow| {
                let name: String = row.try_get(column_name).expect(&format!(
                    "The query\n\n{query}\n\nShould contain a column named {column_name}"
                ));
                return name;
            })
            .fetch_all(pool)
            .await?
            .into_iter()
            .collect();

        let change_status = config_file_manager::ddl_config::update_file_contents_from_db(
            file_loc,
            db_list,
            add_new_as_commented,
            delete_items_from_config,
        )?;

        let added = match change_status.added {
            0 => change_status.added.to_string().bold().yellow(),
            _ => change_status.added.to_string().bold().green(),
        };

        let removed = match change_status.removed {
            0 => change_status.removed.to_string().bold().yellow(),
            _ => change_status.removed.to_string().bold().red(),
        };
        println!(
            "{} config file fetched! Added: {added:<4}, Removed: {removed:<4}",
            list_type
        );

        return Ok(change_status);
    }

    async fn fetch_schema_list(&self, pool: &PgPool) -> Result<SchemaListStatus> {

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("schemas").context("new_items_commented must contain a field schemas")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        let change_status = self
            .fetch_list(
                pool,
                "
                    SELECT nspname schema_name
                    FROM pg_catalog.pg_namespace
                    WHERE nspname NOT ILIKE 'pg_%' AND nspname != 'information_schema'
                    ORDER BY schema_name;
                ",
                "schema_name",
                SCHEMA_CONFIG_LOCATION,
                "\nSchema",
                *new_items_commented,
                delete_items_from_config
            )
            .await?;

        // There was nothing in the schema config file to begin with and so it is the first load.
        if change_status.amount_before_change == 0 {
            return Ok(SchemaListStatus::FirstLoad);
        }

        return Ok(SchemaListStatus::AlreadyLoaded);
    }

    async fn fetch_function_lists(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let mut config_path = format!("./.tusk/config/schemas/{}", schema);
        std::fs::create_dir_all(&config_path)
            .expect("Should be able to create the required directories");
        config_path = config_path + "/functions_to_include.conf";

        // Create the file that will contain the function config if it does not already exist
        if !std::path::Path::new(&config_path).exists() {
            std::fs::write(&config_path, "")?;
        }

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("functions").context("new_items_commented must contain a field functions")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        self.fetch_list(
            pool,
            &format!(
                "
                    SELECT DISTINCT routine_name AS function_name
                    FROM information_schema.routines
                    WHERE (routine_type = 'FUNCTION' OR routine_type = 'PROCEDURE')
                    AND routine_schema = '{}'
                    ORDER BY routine_name
                ",
                schema
            ),
            "function_name",
            &config_path,
            &format!("\t{}: Functions", schema.magenta()),
            *new_items_commented,
            delete_items_from_config
        )
        .await?;

        return Ok(());
    }

    async fn fetch_table_ddl_list(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let mut config_path = format!("./.tusk/config/schemas/{}", schema);
        std::fs::create_dir_all(&config_path)
            .expect("Should be able to create the required directories");
        config_path = config_path + "/table_ddl_to_include.conf";

        // Create the file that will contain the function config if it does not already exist
        if !std::path::Path::new(&config_path).exists() {
            std::fs::write(&config_path, "")?;
        }

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("table_ddl").context("new_items_commented must contain a field table_ddl")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        self.fetch_list(
            pool,
            &format!(
                "
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = '{}'
                        AND table_type ILIKE '%TABLE%';
                    ",
                schema
            ),
            "table_name",
            &config_path,
            &format!("\t{}: Table DDL", schema.magenta()),
            *new_items_commented,
            delete_items_from_config
        )
        .await?;

        return Ok(());
    }

    async fn fetch_table_data_list(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let mut config_path = format!("./.tusk/config/schemas/{}", schema);
        std::fs::create_dir_all(&config_path)
            .expect("Should be able to create the required directories");
        config_path = config_path + "/table_data_to_include.conf";

        // Create the file that will contain the function config if it does not already exist
        if !std::path::Path::new(&config_path).exists() {
            std::fs::write(&config_path, "")?;
        }

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("table_data").context("new_items_commented must contain a field table_data")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        self.fetch_list(
            pool,
            &format!(
                "
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = '{}'
                        AND table_type ILIKE '%TABLE%';
                    ",
                schema
            ),
            "table_name",
            &config_path,
            &format!("\t{}: Table data", schema.magenta()),
            *new_items_commented,
            delete_items_from_config
        )
        .await?;

        return Ok(());
    }
    async fn fetch_data_types_list(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let mut config_path = format!("./.tusk/config/schemas/{}", schema);
        std::fs::create_dir_all(&config_path)
            .expect("Should be able to create the required directories");
        config_path = config_path + "/data_types_to_include.conf";

        // Create the file that will contain the function config if it does not already exist
        if !std::path::Path::new(&config_path).exists() {
            std::fs::write(&config_path, "")?;
        }

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("data_types").context("new_items_commented must contain a field data_types")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        self.fetch_list(
                pool,
                &format!(
                    "
                        SELECT t.typname as data_type 
                        FROM pg_type t 
                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace 
                        WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) 
                        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
                        AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                        AND n.nspname = '{}';
                    ",
                    schema
                ),
                "data_type",
                &config_path,
                &format!("\t{}: Data type", schema.magenta()),
                *new_items_commented,
                delete_items_from_config
            )
            .await?;

        return Ok(());
    }

    async fn fetch_views_list(&self, pool: &PgPool, schema: &str) -> Result<()> {
        let mut config_path = format!("./.tusk/config/schemas/{}", schema);
        std::fs::create_dir_all(&config_path)
            .expect("Should be able to create the required directories");
        config_path = config_path + "/views_to_include.conf";

        // Create the file that will contain the function config if it does not already exist
        if !std::path::Path::new(&config_path).exists() {
            std::fs::write(&config_path, "")?;
        }

        let config = UserConfig::get_global()?;
        let new_items_commented = config.fetch_options.new_items_commented.get("views").context("new_items_commented must contain a field views")?;
        let delete_items_from_config = config.fetch_options.delete_items_from_config;

        self.fetch_list(
            pool,
            &format!(
                "
                    SELECT c.relname as views
                    FROM pg_class c
                    JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace 
                    WHERE ns.nspname = '{}'
                    AND relkind IN ('m', 'v')
                    ORDER BY views
                    ",
                schema
            ),
            "views",
            &config_path,
            &format!("\t{}: Views", schema.magenta()),
            *new_items_commented,
            delete_items_from_config
        )
        .await?;

        return Ok(());
    }
}

#[async_trait]
impl Action for Fetch {
    async fn execute(&self) -> Result<()> {
        println!("\nBeginning Inventory Fetch:");

        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();

        if let SchemaListStatus::FirstLoad = self.fetch_schema_list(pool).await? {
            println!("\n\nThe list of schemas has been initialised at {}\n\nPlease comment out using // any schemas you do not wish to back up before running fetch again. This will create the lists of functions and tables for you to configure", (std::env::current_dir().unwrap().to_str().unwrap().to_owned() + &SCHEMA_CONFIG_LOCATION[1..]).bold());

            return Ok(());
        }

        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        for schema in approved_schemas {
            println!("\nBeginning {} schema fetch:", schema);
            self.fetch_function_lists(pool, &schema).await?;
            self.fetch_table_ddl_list(pool, &schema).await?;
            self.fetch_table_data_list(pool, &schema).await?;
            self.fetch_data_types_list(pool, &schema).await?;
            self.fetch_views_list(pool, &schema).await?;
            println!();
        }

        return Ok(());
    }
}
