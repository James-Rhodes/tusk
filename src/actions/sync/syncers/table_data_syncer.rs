use anyhow::Result;
use sqlx::PgPool;

use crate::{
    actions::sync::{
        syncers::{RowStream, Syncer},
        DDL,
    },
    config_file_manager::{format_config_file, get_uncommented_file_contents},
};

pub struct TableDataSyncer {}

impl Syncer for TableDataSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>> {
        let file_path = format!(
            "./.tusk/config/schemas/{}/table_data_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
            ")
            .bind(schema)
            .bind(approved_tables)
            .fetch(pool));
    }

    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {
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
            "./.tusk/config/schemas/{}/table_data_to_include.conf",
            schema
        );
        format_config_file(&file_path)?;

        let approved_tables = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
        ")
            .bind(schema)
            .bind(items)
            .bind(approved_tables)
            .fetch(pool));
    }
}
