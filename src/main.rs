use clap::Parser;


use tusk::{cli, actions::init::USER_CONFIG_LOCATION};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = cli::CliArgs::parse();

    if std::path::Path::new(USER_CONFIG_LOCATION).exists() {
        // If the user config file exists try read it and initailise the global variable
        tusk::config_file_manager::user_config::UserConfig::init(USER_CONFIG_LOCATION)?;
    }

    args.action.execute().await?;
    Ok(())
}
