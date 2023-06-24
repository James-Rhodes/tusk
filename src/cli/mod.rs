use clap::{Parser, Subcommand};

use crate::actions::{init::Init,pull::Pull, push::Push};
use crate::actions::fetch::Fetch;
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

    /// Pull the DDL from the database
    Pull(Pull),

    /// Push the changes in the local function DDL to the database
    Push(Push),

    /// Fetches a list of all schemas, tables, views and functions within the Database defined by the
    /// connection in ./.tusk/.env
    Fetch(Fetch),
}

impl Action {
    pub async fn execute(&self) -> anyhow::Result<()> {
        match self {
            Self::Init(init) => init.execute(),
            Self::Pull(pull) => pull.execute(),
            Self::Push(push) => push.execute(),
            Self::Fetch(fetch) => fetch.execute(),
        }
        .await?;

        return Ok(());
    }
}
