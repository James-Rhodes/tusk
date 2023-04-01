use anyhow;

pub fn init_directory_structure() -> anyhow::Result<()> {
    std::fs::create_dir_all("./.dbtvc")?;

    if !std::path::Path::new("./.dbtvc/.env").exists() {
        std::fs::write(
            "./.dbtvc/.env",
            "DB_USER=****\nDB_PASSWORD=****\nDB_PORT=****\nDB_NAME=****",
        )?;
    }

    std::fs::create_dir_all("./schemas")?;

    return Ok(());
}

pub fn create_schema_dirs(schema_name: &str) -> anyhow::Result<()> {
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/functions")?;
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/table_data")?;
    std::fs::create_dir_all(String::from("./schemas/") + schema_name + "/table_definition")?;
    return Ok(());
}
