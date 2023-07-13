pub mod data_type_puller;
pub mod function_puller;
pub mod table_data_puller;
pub mod table_ddl_puller;
pub mod view_puller;

use std::pin::Pin;

use crate::{
    actions::pull::DDL,
    config_file_manager::user_config::UserConfig,
};
use anyhow::Result;
use async_trait::async_trait;
use colored::Colorize;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait SQLPuller {
    // This is the only function that is required to be implemented. It must return the query that
    // will get the ddl for the given type. ie function etc. Use $1 to represent the schema and $2
    // to represent the items that you are getting the ddl for. These are bound automatically
    fn get_ddl_query() -> &'static str;

    // This returns the DDL from a Postgres query as a stream for writing manually to a file
    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn [String],
    ) -> Result<RowStream<'conn>> {
        Ok(sqlx::query_as::<_, DDL>(Self::get_ddl_query())
            .bind(schema)
            .bind(items)
            .fetch(pool))
    }
}

#[async_trait]
pub trait PgDumpPuller: Send + 'static {
    /// This is the function that needs to be implemented per puller. It needs to return the
    /// arguments required for pg_dump
    fn pg_dump_arg_gen(schema: &str, item_name: &str) -> Vec<String>;

    // This gets all of the ddl for the input items
    async fn get(
        schema: &str,
        ddl_parent_dir: &str,
        connection_string: &str,
        pg_bin_path: &str,
        items: &[String],
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(ddl_parent_dir)?;
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

    fn get_ddl_from_bytes(ddl_bytes: &[u8]) -> Result<&str> {
        let ddl = std::str::from_utf8(ddl_bytes)?;

        if let Some(end_of_header_pos) = ddl.find("SET") {
            return Ok(&ddl[end_of_header_pos..]);
        }

        Ok(ddl)
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
        let ddl_args = Self::pg_dump_arg_gen(&schema, &item);
        args.extend(ddl_args.into_iter());

        let user_args = UserConfig::get_global()?
            .pull_options
            .pg_dump_additional_args
            .clone();
        args.extend(user_args.into_iter());

        let command = tokio::process::Command::new(pg_bin_path)
            .args(args)
            .output()
            .await?;

        let command_err = std::str::from_utf8(&command.stderr[..]).unwrap_or("");
        if !command_err.is_empty() {
            let command_err = command_err.trim_end().replace('\n', "\n\t\t");
            println!(
                "\t{} ({}/{}.sql): {}",
                "Warning".yellow(),
                ddl_parent_dir,
                item,
                command_err
            );
            return Ok(());
        }
        let command_out = command.stdout;

        let ddl = Self::get_ddl_from_bytes(&command_out)?;

        tokio::fs::write(&file_path, ddl).await?;
        println!("\tPulling {}", file_path.magenta());
        Ok(())
    }
}
