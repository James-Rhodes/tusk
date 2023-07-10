use crate::actions::pull::pullers::PgDumpPuller;

pub struct TableDataPuller {}

impl PgDumpPuller for TableDataPuller {
    fn pg_dump_arg_gen(schema: &str, item_name:&str) -> Vec<String> {
        vec![String::from("--column-inserts"), String::from("--no-owner"), String::from("--data-only"), format!("--table={}.{}", schema, item_name)]
    }
}
