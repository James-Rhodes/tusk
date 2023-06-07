use async_trait::async_trait;
use clap::Args;
use colored::Colorize;

use crate::actions::Action;

pub const ENV_LOCATION: &str = "./.tusk/.env";
pub const SCHEMA_CONFIG_LOCATION: &str = "./.tusk/config/schemas_to_include.conf";

#[derive(Debug, Args)]
pub struct Init {}

impl Init {
    pub fn init_directories(&self) -> anyhow::Result<()> {
        colored::control::set_override(true);

        std::fs::create_dir_all("./.tusk/config/schemas")?;
        println!("\tCreated directory: {}", "./.tusk/config/schemas".bold());

        // Create the .env file for db config info
        if !std::path::Path::new(ENV_LOCATION).exists() {
            std::fs::write(
                ENV_LOCATION,
                "DB_USER=****\nDB_PASSWORD=****\nDB_HOST=****\nDB_PORT=****\nDB_NAME=****\n\n#USE_SSH=FALSE\nSSH_REMOTE_IP_ADDRESS=****\nSSH_USERNAME=****\nSSH_LOCAL_PORT=****\nSSH_REMOTE_PORT=****\n\n#PG_BIN_PATH=****",
            )?;
            println!("\tCreated file: {}", ENV_LOCATION.bold());
        }

        // Create the file that will contain which schemas to include
        if !std::path::Path::new(SCHEMA_CONFIG_LOCATION).exists() {
            std::fs::write(SCHEMA_CONFIG_LOCATION, "")?;
            println!("\tCreated file: {}", SCHEMA_CONFIG_LOCATION.bold());
        }

        std::fs::create_dir_all("./schemas")?;
        println!("\tCreated directory: {}", "./schemas".bold());

        return Ok(());
    }
}

#[async_trait]
impl Action for Init {
    async fn execute(&self) -> anyhow::Result<()> {
        println!(
            "\nInitialising the required directory structure and creating template .env file..."
        );
        self.init_directories()?;
        println!("Finished initialisation\n");

        return Ok(());
    }
}
