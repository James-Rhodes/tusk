use crate::actions::sync::syncers::SQLSyncer;

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
    fn get_ddl_query() -> &'static str {
        return FUNCTION_DDL_QUERY;
    }
}
