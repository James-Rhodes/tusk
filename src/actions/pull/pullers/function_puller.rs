use crate::actions::pull::pullers::SQLPuller;

const FUNCTION_DDL_QUERY: &str = "
        SELECT
            format('%I(%s)', 
            p.proname, 
            oidvectortypes(p.proargtypes)) AS name,
            pg_get_functiondef(p.oid) AS definition,
            format('functions/%I/%I(%s)', p.proname, p.proname, oidvectortypes(p.proargtypes)) AS file_path
        FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
        WHERE ns.nspname = $1
        AND p.proname IN (SELECT * FROM UNNEST($2))
        AND p.prokind IN ('f', 'p') -- This only supports functions and procedures. Not aggregates
        UNION
        SELECT '', '', format('functions/%s', func_name)
        FROM (
            SELECT 
                * 
            FROM UNNEST($2) func_name
        ) names
        WHERE names.func_name NOT IN (
            SELECT
                p.proname
            FROM pg_proc p INNER JOIN pg_namespace ns ON (p.pronamespace = ns.oid)
            WHERE ns.nspname = $1
        ) 
            ";

pub struct FunctionPuller {}

impl SQLPuller for FunctionPuller {
    fn get_ddl_query() -> &'static str {
        FUNCTION_DDL_QUERY
    }
}
