use sqlx::PgPool;
use anyhow::Result;

use crate::{actions::sync::{
    syncers::{RowStream, Syncer},
    DDL,
}, config_file_manager::{get_uncommented_file_contents, format_config_file}};

pub struct FunctionSyncer {}

impl Syncer for FunctionSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>> {

        let file_path =  format!( "./.dbtvc/config/schemas/{}/functions_to_include.conf", schema);
        format_config_file(&file_path)?;

        let approved_funcs = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
            SELECT
                format('%I(%s)', p.proname, oidvectortypes(p.proargtypes)) name,
                pg_get_functiondef(p.oid) definition,
                format('functions/%I/%I(%s)', p.proname, p.proname, oidvectortypes(p.proargtypes)) file_path
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE ns.nspname = $1
            AND p.proname IN (SELECT * FROM UNNEST($2));")
            .bind(schema)
            .bind(approved_funcs)
            .fetch(pool));
    }

    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {
        // TODO: See if this can be changed to have no clones/collects
        let items = items.iter().map(|item| {
            let mut new_item = item.clone();
            new_item.push('%');
            return new_item;
        }).collect::<Vec<String>>();

        let file_path =  format!( "./.dbtvc/config/schemas/{}/functions_to_include.conf", schema);
        format_config_file(&file_path)?;

        let approved_funcs = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>("
            SELECT
                format('%I(%s)', p.proname, oidvectortypes(p.proargtypes)) name,
                pg_get_functiondef(p.oid) definition,
                format('functions/%I/%I(%s)', p.proname, p.proname, oidvectortypes(p.proargtypes)) file_path
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE ns.nspname = $1
            AND p.proname ILIKE ANY(SELECT * FROM UNNEST($2))
            AND p.proname IN (SELECT * FROM UNNEST($3));
        ")
            .bind(schema)
            .bind(items)
            .bind(approved_funcs)
            .fetch(pool));
    }
}
