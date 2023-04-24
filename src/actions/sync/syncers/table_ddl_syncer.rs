use anyhow::Result;

use crate::{
    actions::sync::syncers::PgDumpSyncer,
    config_file_manager::{format_config_file, get_uncommented_file_contents},
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

        return Ok(());
    }

    fn get(
        schema: &str,
        items: &Vec<String>,
    ) -> Result<()> {
        // TODO: See if this can be changed to have no clones/collects
        let items = items
            .iter()
            .map(|item| {
                let mut new_item = item.clone();
                new_item.push('%');
                return new_item;
            })
            .collect::<Vec<String>>();

        let file_path = format!(
            "./.tusk/config/schemas/{}/table_ddl_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        return Ok(());
    }
}
