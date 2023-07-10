use crate::actions::pull::pullers::PgDumpPuller;

pub struct ViewPuller {}

impl PgDumpPuller for ViewPuller {
    fn pg_dump_arg_gen(schema: &str, item_name:&str) -> Vec<String> {
        vec![String::from("--schema-only"), String::from("--no-owner"), format!("--table={}.{}", schema, item_name)]
    }
}
