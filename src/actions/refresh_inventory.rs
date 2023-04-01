use super::Action;
use clap::Args;

#[derive(Debug, Args)]
pub struct RefreshInventory {}

impl Action for RefreshInventory {
    fn execute(&self) -> anyhow::Result<()> {
        return Ok(());
    }
}
