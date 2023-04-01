use clap::{Parser,Subcommand};

use crate::actions::Action as CliAction;
use crate::actions::init::Init;

#[derive(Debug, Parser)]
#[clap(author,version,about)]
pub struct CliArgs {
    #[clap(subcommand)]
    pub action: Action
}


#[derive(Debug,Subcommand)]
pub enum Action {
    /// Initialise the current directory for version control. Creates all necessary config files
    /// and a schema folder.
    Init(Init),

    /// Sync the local database DDL with what is currently on the database.
    Sync,

    /// Push the changes in the local DDL to the database
    Push,

    /// Creates a list of all schemas, tables and functions within the Database defined by the
    /// connection in ./.dbtvc/.env
    RefreshInventory
}

impl Action {
    pub fn execute(&self) -> anyhow::Result<()>{
        match self {
            Self::Init(init) => init.execute(),
            Self::Sync => todo!(),
            Self::Push => todo!(),
            Self::RefreshInventory => todo!()
        }?;

        return Ok(());

    }
}
