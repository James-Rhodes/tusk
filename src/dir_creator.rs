pub fn create_schema_dirs(schema_name: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/functions")?;
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/table_data")?;
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/table_definition")?;
    return Ok(());
}
