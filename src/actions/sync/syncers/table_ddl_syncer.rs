use crate::actions::sync::syncers::PgDumpSyncer;

pub struct TableDDLSyncer {}

impl PgDumpSyncer for TableDDLSyncer {
    fn pg_dump_arg_gen(schema: &str, item_name:&str) -> Vec<String> {
        return vec![String::from("--schema-only"), String::from("--no-owner"), format!("--table={}.{}", schema, item_name)];
    }
}
