pub mod data_type_syncer;
pub mod function_syncer;
pub mod table_data_syncer;
pub mod table_ddl_syncer;
pub mod view_syncer;

use std::pin::Pin;

use crate::{
    actions::sync::DDL,
    config_file_manager::{
        format_config_file, get_matching_file_contents, get_uncommented_file_contents,
    },
};
use anyhow::Result;
use colored::Colorize;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait SQLSyncer {
    // This returns all the DDL from a postgres query as a stream for writing manually to a file
    fn get_all<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        config_file_path: &str,
    ) -> Result<RowStream<'conn>> {
        format_config_file(&config_file_path)?;

        let approved_data_types = get_uncommented_file_contents(&config_file_path)?;

        return Ok(sqlx::query_as::<_, DDL>(Self::get_ddl_query())
            .bind(schema)
            .bind(approved_data_types)
            .fetch(pool));
    }

    // This returns the DDL from a Postgres query as a stream for writing manually to a file
    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        config_file_path: &str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {

        format_config_file(&config_file_path)?;

        let approved_data_types = get_uncommented_file_contents(&config_file_path)?;
        let items =
            get_matching_file_contents(&approved_data_types, &items, Some(schema))?
                .into_iter()
                .map(|item| item.clone())
                .collect::<Vec<String>>();

        return Ok(sqlx::query_as::<_, DDL>(Self::get_ddl_query())
            .bind(schema)
            .bind(items)
            .fetch(pool));
    }

    // This is the only function that is required to be implemented. It must return the query that
    // will get the ddl for the given type. ie function etc. Use $1 to represent the schema and $2
    // to represent the items that you are getting the ddl for. These are bound automatically
    fn get_ddl_query() -> &'static str;
}

pub trait PgDumpSyncer {
    /// This is the function that needs to be implemented per syncer. It needs to return the
    /// arguments required for pg_dump
    fn pg_dump_arg_gen(schema: &str, item_name: &str) -> Vec<String>;

    fn get_all(
        schema: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
    ) -> Result<()> {
        format_config_file(&config_file_path)?;

        let approved_items = get_uncommented_file_contents(&config_file_path)?;

        if approved_items.len() == 0 {
            return Ok(());
        }

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(&ddl_parent_dir)?;
        }

        let db_name_arg = format!("--dbname={}", connection_string);

        for item in approved_items {
            let file_path = format!("{}/{}.sql", &ddl_parent_dir, item);
            println!("\tSyncing {}", file_path.magenta());

            let mut args = vec![&db_name_arg];
            let user_args = Self::pg_dump_arg_gen(schema, &item);
            args.extend(user_args.iter());

            let command_out = std::process::Command::new("pg_dump")
                .args(args)
                .output()?
                .stdout;

            let ddl = Self::get_ddl_from_bytes(&command_out)?;

            std::fs::write(&file_path, ddl)?;
        }
        return Ok(());
    }

    // This one will write the ones in items to DDL files
    fn get(
        schema: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        items: &Vec<String>,
    ) -> Result<()> {
        format_config_file(&config_file_path)?;

        let approved_tables = get_uncommented_file_contents(&config_file_path)?;

        let items = get_matching_file_contents(&approved_tables, items, Some(schema))?;

        if items.is_empty() {
            return Ok(());
        }

        // TODO: Make this async as well

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(&ddl_parent_dir)?;
        }

        let db_name_arg = format!("--dbname={}", connection_string);

        for item in items {
            let file_path = format!("{}/{}.sql", &ddl_parent_dir, item);
            println!("\tSyncing {}", file_path.magenta());
            let mut args = vec![&db_name_arg];
            let user_args = Self::pg_dump_arg_gen(schema, &item);
            args.extend(user_args.iter());

            let command_out = std::process::Command::new("pg_dump")
                .args(args)
                .output()?
                .stdout;

            let ddl = Self::get_ddl_from_bytes(&command_out)?;

            std::fs::write(&file_path, ddl)?;
        }

        return Ok(());
    }

    fn get_ddl_from_bytes<'d>(ddl_bytes: &'d Vec<u8>) -> Result<&'d str> {
        let ddl = std::str::from_utf8(&ddl_bytes)?;
        let end_of_header_pos = ddl
            .find("SET")
            .expect("There should be a SET statement at the start of the DDL");

        return Ok(&ddl[end_of_header_pos..]);
    }
}
