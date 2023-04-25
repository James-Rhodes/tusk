use anyhow::Result;

use crate::{
    actions::sync::syncers::PgDumpSyncer,
    config_file_manager::{format_config_file, get_uncommented_file_contents},
    db_manager::get_connection_string,
};

pub struct TableDDLSyncer {}

impl PgDumpSyncer for TableDDLSyncer {
    fn get_all(schema: &str) -> Result<()> {
        let file_path = format!(
            "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        if approved_tables.len() == 0 {
            return Ok(());
        }

        // TODO: Make this async as well
        let parent_dir = format!("./schemas/{}/table_ddl", schema);

        if !std::path::Path::new(&parent_dir).exists() {
            std::fs::create_dir_all(&parent_dir)?;
        }

        let db_name = format!("--dbname={}", get_connection_string()?);
        let schema_arg = format!("--schema={}", schema);

        for table in approved_tables {
            let file_path = format!("{}/{}.sql", &parent_dir, table);
            let table_arg = format!("--table={}", table);

            let command_out = std::process::Command::new("pg_dump")
                .args([
                    &db_name,
                    "--schema-only",
                    "--no-owner",
                    &schema_arg,
                    &table_arg,
                ])
                .output()?
                .stdout;

            let ddl = Self::get_ddl_from_bytes(&command_out)?;

            std::fs::write(&file_path, ddl)?;
        }

        return Ok(());
    }

    fn get(schema: &str, items: &Vec<String>) -> Result<()> {
        let file_path = format!(
            "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        // TODO: See if this can be changed to have no clones/collects
        let mut items = items
            .iter()
            .filter_map(|item| {
                let matches_approved = approved_tables
                    .iter()
                    .filter(|table| table.starts_with(item))
                    .collect::<Vec<&String>>();

                if matches_approved.len() != 0 {
                    return Some(matches_approved);
                }

                return None;
            })
            .flatten()
            .collect::<Vec<&String>>();
        items.sort();
        items.dedup();

        if items.len() == 0 {
            return Ok(());
        }

        // TODO: Make this async as well
        let parent_dir = format!("./schemas/{}/table_ddl", schema);

        if !std::path::Path::new(&parent_dir).exists() {
            std::fs::create_dir_all(&parent_dir)?;
        }

        let db_name = format!("--dbname={}", get_connection_string()?);
        let schema_arg = format!("--schema={}", schema);

        for table in items {
            let file_path = format!("{}/{}.sql", &parent_dir, table);
            let table_arg = format!("--table={}", table);

            let command_out = std::process::Command::new("pg_dump")
                .args([
                    &db_name,
                    "--schema-only",
                    "--no-owner",
                    &schema_arg,
                    &table_arg,
                ])
                .output()?
                .stdout;

            let ddl = Self::get_ddl_from_bytes(&command_out)?;

            std::fs::write(&file_path, ddl)?;
        }

        return Ok(());
    }
}
