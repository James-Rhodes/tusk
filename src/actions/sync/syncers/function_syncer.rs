use sqlx::PgPool;
use anyhow::Result;

use crate::{actions::sync::{
    syncers::{RowStream, SQLSyncer},
    DDL,
}, config_file_manager::{get_uncommented_file_contents, format_config_file, get_matching_uncommented_file_contents}};

const FUNCTION_DDL_QUERY: &str = "
            SELECT
                format('%I(%s)', p.proname, oidvectortypes(p.proargtypes)) name,
                pg_get_functiondef(p.oid) definition,
                format('functions/%I/%I(%s)', p.proname, p.proname, oidvectortypes(p.proargtypes)) file_path
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE ns.nspname = $1
            AND p.proname IN (SELECT * FROM UNNEST($2))
            ";

pub struct FunctionSyncer {}

impl SQLSyncer for FunctionSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>> {

        let file_path =  format!( "./.tusk/config/schemas/{}/functions_to_include.conf", schema);
        format_config_file(&file_path)?;

        let approved_funcs = get_uncommented_file_contents(&file_path)?;

        return Ok(sqlx::query_as::<_, DDL>(FUNCTION_DDL_QUERY)
            .bind(schema)
            .bind(approved_funcs)
            .fetch(pool));
    }

    fn get<'conn>(
        pool: &'conn PgPool,
        schema: &'conn str,
        items: &'conn Vec<String>,
    ) -> Result<RowStream<'conn>> {

        let file_path =  format!( "./.tusk/config/schemas/{}/functions_to_include.conf", schema);
        format_config_file(&file_path)?;

        let approved_funcs = get_uncommented_file_contents(&file_path)?;
        let items =
            get_matching_uncommented_file_contents(&approved_funcs, &items, Some(schema))?
                .into_iter()
                .map(|item| item.clone())
                .collect::<Vec<String>>();

        return Ok(sqlx::query_as::<_, DDL>(FUNCTION_DDL_QUERY)
            .bind(schema)
            .bind(items)
            .fetch(pool));
    }
}
