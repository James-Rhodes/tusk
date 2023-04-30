use crate::actions::sync::syncers::PgDumpSyncer;

pub struct TableDataSyncer {}

impl PgDumpSyncer for TableDataSyncer {
    fn pg_dump_arg_gen(schema: &str, item_name:&str) -> Vec<String> {
        return vec![String::from("--column-inserts"), String::from("--no-owner"), String::from("--data-only"), format!("--table={}.{}", schema, item_name)];
    }
}
