use clap::{Parser, Subcommand};

use crate::actions::{init::Init,sync::Sync, push::Push};
use crate::actions::refresh_inventory::RefreshInventory;
use crate::actions::Action as CliAction;

#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub struct CliArgs {
    #[clap(subcommand)]
    pub action: Action,
}

#[derive(Debug, Subcommand)]
pub enum Action {
    /// Initialise the current directory for version control. Creates all necessary config files
    /// and a schema folder.
    Init(Init),

    /// Sync the local database DDL with what is currently on the database.
    Sync(Sync),

    /// Push the changes in the local function DDL to the database
    Push(Push),

    /// Creates a list of all schemas, tables, views and functions within the Database defined by the
    /// connection in ./.tusk/.env
    RefreshInventory(RefreshInventory),
}

impl Action {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::Init(init) => init.execute(),
            Self::Sync(sync) => sync.execute(),
            Self::Push(push) => push.execute(),
            Self::RefreshInventory(ri) => ri.execute(),
        }
        .await?;

        return Ok(());
    }
}
