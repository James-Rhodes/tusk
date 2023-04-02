use clap::Parser;
use anyhow;

use dbtvc::cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = cli::CliArgs::parse();
    println!("{:?}", args);
    args.action.execute().await?;
    Ok(())
}
