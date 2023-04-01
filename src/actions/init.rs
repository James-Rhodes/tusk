use super::Action;
use clap::Args;

#[derive(Debug, Args)]
pub struct Init {}

impl Init {
    pub fn init_directories(&self) -> anyhow::Result<()> {
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
}

impl Action for Init {
    fn execute(&self) -> anyhow::Result<()> {

        println!("Initialising the required directory structure and creating template .env file...");
        self.init_directories()?;
        println!("Finished initialisation");

        return Ok(());
    }
}
