use sqlx::PgPool;

use crate::actions::sync::{
    syncers::{RowStream, Syncer},
    DDL,
};

pub struct FunctionSyncer {}

impl Syncer for FunctionSyncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> RowStream<'conn> {
        return sqlx::query_as::<_, DDL>("
            SELECT
                format('%I(%s)', p.proname, oidvectortypes(p.proargtypes)) name,
                pg_get_functiondef(p.oid) definition,
                format('functions/%I/%I(%s)', p.proname, p.proname, oidvectortypes(p.proargtypes)) file_path
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE ns.nspname = $1;")
            .bind(schema)
            .fetch(pool);

    }

    fn get<'conn>(pool: &'conn PgPool, schema: &str, items: Vec<String>) -> RowStream<'conn> {
        todo!();
    }
}
