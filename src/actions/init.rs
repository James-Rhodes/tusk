use clap::Args;
use colored::Colorize;

pub const ENV_LOCATION: &str = "./.tusk/.env";
pub const USER_CONFIG_LOCATION: &str = "./.tusk/user_config.yaml";
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
                "DB_USER=****\nDB_PASSWORD=****\nDB_HOST=****\nDB_PORT=****\nDB_NAME=****\n\n#USE_SSH=FALSE\nSSH_HOST=****\nSSH_USER=****\nSSH_LOCAL_BIND_PORT=****\n\n#PG_BIN_PATH=****",
            )?;
            println!("\tCreated file: {}", ENV_LOCATION.bold());
        }

        // Create the user_config file for user config info
        if !std::path::Path::new(USER_CONFIG_LOCATION).exists() {
            std::fs::write(
                USER_CONFIG_LOCATION,
                r#"
fetch_options:
    new_items_commented: # Decide if new items fetched from the DB should be added to their config files as comments or not
      schemas: true
      functions: false
      table_ddl: false
      table_data: true
      views: false
      data_types: false
    delete_items_from_config: true # Decide if items that exist in the config files but not on the DB should be deleted from the config files on fetch

pull_options:
    clean_ddl_before_pulling: true # Delete files before repopulating with pull. Functions will only be deleted if there aren't unit tests defined for the function
    confirm_before_pull: false # Require confirmation of what ddl will be pulled before commencing pulling 
    pg_dump_additional_args: # These are additional user args that can be added to the calls to pg_dump

push_options:
    test_after_push: true # Run unit tests after pushing the functions. If ANY of the tests fail then all of the functions that were pushed will be rolled back.
    confirm_before_push: false # Require confirmation of what functions will be pushed before commencing pushing 
                "#,
            )?;
            println!("\tCreated file: {}", USER_CONFIG_LOCATION.bold());
        }

        // Create the file that will contain which schemas to include
        if !std::path::Path::new(SCHEMA_CONFIG_LOCATION).exists() {
            std::fs::write(SCHEMA_CONFIG_LOCATION, "")?;
            println!("\tCreated file: {}", SCHEMA_CONFIG_LOCATION.bold());
        }

        std::fs::create_dir_all("./schemas")?;
        println!("\tCreated directory: {}", "./schemas".bold());

        Ok(())
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        println!(
        "\nInitialising the required directory structure and creating template .env file..."
    );
        self.init_directories()?;
        println!("Finished initialisation\n");

        Ok(())
    }
}

