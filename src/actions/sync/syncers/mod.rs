pub mod function_syncer;
pub mod table_data_syncer;
pub mod table_ddl_syncer;

use std::pin::Pin;

use crate::{actions::sync::DDL, config_file_manager::{format_config_file, get_uncommented_file_contents, get_matching_uncomented_file_contents}};
use anyhow::Result;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait SQLSyncer {
    // This returns all the DDL from a postgres query as a stream for writing manually to a file
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>>;

    // This returns the DDL from a Postgres query as a stream for writing manually to a file
    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>>;
}

pub trait PgDumpSyncer {

    /// This is the function that needs to be implemented per syncer. It needs to return the
    /// arguments required for pg_dump
    fn pg_dump_arg_gen(schema: &str, item_name:&str) -> Vec<String>;

    fn get_all(schema: &str, config_file_path: &str, ddl_parent_dir: &str, connection_string: &str) -> Result<()> {

        format_config_file(&config_file_path)?;

        let approved_items = get_uncommented_file_contents(&config_file_path)?;

        if approved_items.len() == 0 {
            return Ok(());
        }

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(&ddl_parent_dir)?;
        }

        let db_name_arg = format!("--dbname={}", connection_string);
        // Remember the command for tables is pg_dump connection_string --schema_only --no-owner
        // --table=schema.table

        for item in approved_items {

            let file_path = format!("{}/{}.sql", &ddl_parent_dir, item);

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
    fn get(schema: &str, config_file_path: &str, ddl_parent_dir: &str, connection_string: &str, items: &Vec<String>) -> Result<()> {

        format_config_file(&config_file_path)?;

        let approved_tables = get_uncommented_file_contents(&config_file_path)?;

        let items = get_matching_uncomented_file_contents(&approved_tables, items, Some(schema))?;

        if items.is_empty() {
            return Ok(());
        }

        // TODO: Make this async as well

        if !std::path::Path::new(&ddl_parent_dir).exists() {
            std::fs::create_dir_all(&ddl_parent_dir)?;
        }

        let db_name_arg = format!("--dbname={}", connection_string);
// TODO: Work on making this maybe generic between the two different get and get all
        for item in items {

            let file_path = format!("{}/{}.sql", &ddl_parent_dir, item);
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

// impl PgDumpSyncer {
//     // This one will write all of the DDL to files
//     fn get_all(schema: &str, config_file_path: &str, ddl_parent_dir: &str, connection_string: &str, pg_dump_arg_gen: fn(&str, &str) -> Vec<String>) -> Result<()> {
//
//         format_config_file(&config_file_path)?;
//
//         let approved_items = get_uncommented_file_contents(&config_file_path)?;
//
//         if approved_items.len() == 0 {
//             return Ok(());
//         }
//
//         if !std::path::Path::new(&ddl_parent_dir).exists() {
//             std::fs::create_dir_all(&ddl_parent_dir)?;
//         }
//
//         let db_name_arg = format!("--dbname={}", connection_string);
//         // Remember the command for tables is pg_dump connection_string --schema_only --no-owner
//         // --table=schema.table
//
//         for item in approved_items {
//
//             let file_path = format!("{}/{}.sql", &ddl_parent_dir, item);
//
//             let args = vec![db_name_arg];
//             args.extend(pg_dump_arg_gen(schema, &item).iter());
//
//             let command_out = std::process::Command::new("pg_dump")
//                 .args(args)
//                 .output()?
//                 .stdout;
//
//             let ddl = Self::get_ddl_from_bytes(&command_out)?;
//
//             std::fs::write(&file_path, ddl)?;
//         }
//         return Ok(());
//     }
//
//     // This one will write the ones in items to DDL files
//     fn get(schema: &str, items: &Vec<String>) -> Result<()> {
//         let file_path = format!(
//             "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
//             schema
//         );
//         format_config_file(&file_path)?;
//
//         let approved_tables = get_uncommented_file_contents(&file_path)?;
//
//         let items = get_matching_uncomented_file_contents(&approved_tables, items, Some(schema))?;
//
//         if items.is_empty() {
//             return Ok(());
//         }
//
//         // TODO: Make this async as well
//         let parent_dir = format!("./schemas/{}/table_ddl", schema);
//
//         if !std::path::Path::new(&parent_dir).exists() {
//             std::fs::create_dir_all(&parent_dir)?;
//         }
//
//         let db_name_arg = format!("--dbname={}", get_connection_string()?);
// // TODO: Work on making this maybe generic between the two different get and get all
//         for table in items {
//             let file_path = format!("{}/{}.sql", &parent_dir, table);
//             let table_arg = format!("--table={}.{}", schema, table);
//
//             let command_out = std::process::Command::new("pg_dump")
//                 .args([&db_name_arg, "--schema-only", "--no-owner", &table_arg])
//                 .output()?
//                 .stdout;
//
//             let ddl = Self::get_ddl_from_bytes(&command_out)?;
//
//             std::fs::write(&file_path, ddl)?;
//         }
//
//         return Ok(());
//     }
//
//     fn get_ddl_from_bytes<'d>(ddl_bytes: &'d Vec<u8>) -> Result<&'d str> {
//         let ddl = std::str::from_utf8(&ddl_bytes)?;
//         let end_of_header_pos = ddl
//             .find("SET")
//             .expect("There should be a SET statement at the start of the DDL");
//
//         return Ok(&ddl[end_of_header_pos..]);
//     }
// }
