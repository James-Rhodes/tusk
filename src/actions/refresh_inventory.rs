use std::collections::HashSet;

use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::actions::{init::SCHEMA_CONFIG_LOCATION, Action};
use crate::config_file_manager::get_uncommented_file_contents;
use crate::{config_file_manager, db_manager};

enum SchemaListStatus {
    FirstLoad,
    AlreadyLoaded,
}

#[derive(Debug, Args)]
pub struct RefreshInventory {}

impl RefreshInventory {
    async fn refresh_list(
        &self,
        pool: &PgPool,
        query: &str,
        column_name: &str,
        file_loc: &str,
        list_type: &str,
        add_new_as_commented: bool,
    ) -> Result<config_file_manager::ChangeStatus> {
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

        let change_status = config_file_manager::update_file_contents_from_db(
            file_loc,
            db_list,
            add_new_as_commented,
        )?;

        println!(
            "{} config file refreshed! Added: {}, Removed: {}",
            list_type, change_status.added, change_status.removed
        );

        return Ok(change_status);
    }

    async fn refresh_schema_list(&self, pool: &PgPool) -> Result<SchemaListStatus> {
        let change_status = self
            .refresh_list(
                pool,
                "
                    SELECT nspname schema_name
                    FROM pg_catalog.pg_namespace
                    WHERE nspname NOT ILIKE 'pg_%' AND nspname != 'information_schema'
                    ORDER BY schema_name;
                ",
                "schema_name",
                SCHEMA_CONFIG_LOCATION,
                "Schema",
                true,
            )
            .await?;

        // There was nothing in the schema config file to begin with and so it is the first load.
        if change_status.amount_before_change == 0 {
            return Ok(SchemaListStatus::FirstLoad);
        }

        return Ok(SchemaListStatus::AlreadyLoaded);
    }

    async fn refresh_function_lists(&self, pool: &PgPool) -> Result<()> {
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION);

        for schema in approved_schemas? {
            let mut config_path = format!("./.tusk/config/schemas/{}", schema);
            std::fs::create_dir_all(&config_path)
                .expect("Should be able to create the required directories");
            config_path = config_path + "/functions_to_include.conf";

            // Create the file that will contain the function config if it does not already exist
            if !std::path::Path::new(&config_path).exists() {
                std::fs::write(&config_path, "")?;
            }

            self.refresh_list(
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
                &format!("The schema: \"{}\" has had its function definition", schema),
                false,
            )
            .await?;
        }

        return Ok(());
    }

    async fn refresh_table_ddl_list(&self, pool: &PgPool) -> Result<()> {
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION);

        for schema in approved_schemas? {
            let mut config_path = format!("./.tusk/config/schemas/{}", schema);
            std::fs::create_dir_all(&config_path)
                .expect("Should be able to create the required directories");
            config_path = config_path + "/table_ddl_to_include.conf";

            // Create the file that will contain the function config if it does not already exist
            if !std::path::Path::new(&config_path).exists() {
                std::fs::write(&config_path, "")?;
            }

            self.refresh_list(
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
                &format!("The schema: \"{}\" has had its table definitions", schema),
                false,
            )
            .await?;
        }

        return Ok(());
    }

    async fn refresh_table_data_list(&self, pool: &PgPool) -> Result<()> {
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION);

        for schema in approved_schemas? {
            let mut config_path = format!("./.tusk/config/schemas/{}", schema);
            std::fs::create_dir_all(&config_path)
                .expect("Should be able to create the required directories");
            config_path = config_path + "/table_data_to_include.conf";

            // Create the file that will contain the function config if it does not already exist
            if !std::path::Path::new(&config_path).exists() {
                std::fs::write(&config_path, "")?;
            }

            self.refresh_list(
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
                &format!("The schema: \"{}\" has had its table data", schema),
                true,
            )
            .await?;
        }

        return Ok(());
    }
    async fn refresh_data_types_list(&self, pool: &PgPool) -> Result<()> {
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION);

        for schema in approved_schemas? {
            let mut config_path = format!("./.tusk/config/schemas/{}", schema);
            std::fs::create_dir_all(&config_path)
                .expect("Should be able to create the required directories");
            config_path = config_path + "/data_types_to_include.conf";

            // Create the file that will contain the function config if it does not already exist
            if !std::path::Path::new(&config_path).exists() {
                std::fs::write(&config_path, "")?;
            }

            self.refresh_list(
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
                &format!("The schema: \"{}\" has had its custom data types", schema),
                false,
            )
            .await?;
        }

        return Ok(());
    }

    async fn refresh_views_list(&self, pool: &PgPool) -> Result<()> {
        let approved_schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION);

        for schema in approved_schemas? {
            let mut config_path = format!("./.tusk/config/schemas/{}", schema);
            std::fs::create_dir_all(&config_path)
                .expect("Should be able to create the required directories");
            config_path = config_path + "/views_to_include.conf";

            // Create the file that will contain the function config if it does not already exist
            if !std::path::Path::new(&config_path).exists() {
                std::fs::write(&config_path, "")?;
            }

            self.refresh_list(
                pool,
                &format!(
                    "
                    SELECT c.oid::regclass::text as views
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
                &format!("The schema: \"{}\" has had its views", schema),
                false,
            )
            .await?;
        }

        return Ok(());
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

        self.refresh_function_lists(&pool).await?;
        self.refresh_table_ddl_list(&pool).await?;
        self.refresh_table_data_list(&pool).await?;
        self.refresh_data_types_list(&pool).await?;
        self.refresh_views_list(&pool).await?;

        return Ok(());
    }
}
