pub mod data_type_puller;
pub mod function_puller;
pub mod table_data_puller;
pub mod table_ddl_puller;
pub mod view_puller;

use std::pin::Pin;

use crate::{
    actions::pull::DDL,
    config_file_manager::ddl_config::{
        format_config_file, get_matching_file_contents, get_uncommented_file_contents,
    },
};
use anyhow::Result;
use async_trait::async_trait;
use colored::Colorize;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait SQLPuller {
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
        let items = get_matching_file_contents(&approved_data_types, &items, Some(schema))?
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

#[async_trait]
pub trait PgDumpPuller: Send + 'static {
    /// This is the function that needs to be implemented per puller. It needs to return the
    /// arguments required for pg_dump
    fn pg_dump_arg_gen(schema: &str, item_name: &str) -> Vec<String>;

    async fn get_all(
        schema: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        pg_bin_path: &str,
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

        let mut join_handles = Vec::with_capacity(approved_items.len());
        for item in approved_items {
            join_handles.push(tokio::spawn(Self::run_pg_dump(
                schema.to_string(),
                pg_bin_path.to_string(),
                db_name_arg.clone(),
                item.to_string(),
                ddl_parent_dir.to_string(),
            )));
        }

        for jh in join_handles {
            jh.await??;
        }
        return Ok(());
    }

    // This one will write the ones in items to DDL files
    async fn get(
        schema: &str,
        config_file_path: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        pg_bin_path: &str,
        items: &Vec<String>,
    ) -> Result<()> {
        format_config_file(&config_file_path)?;

        let approved_tables = get_uncommented_file_contents(&config_file_path)?;

        let items = get_matching_file_contents(&approved_tables, items, Some(schema))?;

        if items.is_empty() {
            return Ok(());
        }

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(&ddl_parent_dir)?;
        }

        let db_name_arg = format!("--dbname={}", connection_string);

        let mut join_handles = Vec::with_capacity(items.len());
        for item in items {
            join_handles.push(tokio::spawn(Self::run_pg_dump(
                schema.to_string(),
                pg_bin_path.to_string(),
                db_name_arg.clone(),
                item.clone(),
                ddl_parent_dir.to_string(),
            )));
        }

        for jh in join_handles {
            jh.await??;
        }

        return Ok(());
    }

    fn get_ddl_from_bytes<'d>(ddl_bytes: &'d Vec<u8>) -> Result<&'d str> {
        let ddl = std::str::from_utf8(&ddl_bytes)?;

        if let Some(end_of_header_pos) = ddl.find("SET") {
            return Ok(&ddl[end_of_header_pos..]);
        }

        return Ok(&ddl);
    }

    async fn run_pg_dump(
        schema: String,
        pg_bin_path: String,
        db_name_arg: String,
        item: String,
        ddl_parent_dir: String,
    ) -> Result<()> {
        let file_path = format!("{}/{}.sql", ddl_parent_dir, item);

        let mut args = vec![db_name_arg.to_owned()];
        let user_args = Self::pg_dump_arg_gen(&schema, &item);
        args.extend(user_args.into_iter());

        let command = tokio::process::Command::new(pg_bin_path)
            .args(args)
            .output()
            .await?;

        let command_err = std::str::from_utf8(&command.stderr[..]).unwrap_or("");
        if command_err.len() > 0 {
            let command_err = command_err.trim_end().replace("\n", "\n\t\t");
            println!("\t{}: {}", "Warning".yellow(), command_err);
            return Ok(());
        }
        let command_out = command.stdout;

        let ddl = Self::get_ddl_from_bytes(&command_out)?;

        tokio::fs::write(&file_path, ddl).await?;
        println!("\tPulling {}", file_path.magenta());
        Ok(())
    }
}
