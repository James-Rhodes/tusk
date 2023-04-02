use async_trait::async_trait;
use clap::Args;

use crate::actions::Action;

#[derive(Debug, Args)]
pub struct Init {}

impl Init {
    pub fn init_directories(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all("./.dbtvc/config/schemas")?;

        // Create the .env file for db config info
        if !std::path::Path::new("./.dbtvc/.env").exists() {
            std::fs::write(
                "./.dbtvc/.env",
                "DB_USER=****\nDB_PASSWORD=****\nDB_PORT=****\nDB_NAME=****",
            )?;
        }

        // Create the file that will contain which schemas to include
        if !std::path::Path::new("./.dbtvc/config/schemas_to_include.conf").exists() {
            std::fs::write("./.dbtvc/config/schemas_to_include.conf", "")?;
        }

        std::fs::create_dir_all("./schemas")?;

        return Ok(());
    }
}

#[async_trait]
impl Action for Init {
    async fn execute(&self) -> anyhow::Result<()> {
        println!(
            "Initialising the required directory structure and creating template .env file..."
        );
        self.init_directories()?;
        println!("Finished initialisation");

        return Ok(());
    }
}
