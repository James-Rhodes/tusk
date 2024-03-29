use clap::{Parser, Subcommand};

use crate::actions::{init::Init,pull::Pull, push::Push, fetch::Fetch, unit_test::UnitTest, doc::Doc};

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

    /// Runs unit tests of each of the defined functions or procedures defined in the unit
    /// test .yaml files
    #[clap(name = "test")]
    UnitTest(UnitTest),

    /// Generate the docs for a given schemas functions
    Doc(Doc)

}

impl Action {
    pub async fn execute(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Init(init) => init.execute().await?,
            Self::Pull(pull) => pull.execute().await?,
            Self::Push(push) => push.execute().await?,
            Self::Fetch(fetch) => fetch.execute().await?,
            Self::UnitTest(unit_test) => unit_test.execute().await?,
            Action::Doc(doc) => doc.execute().await?,
        };
        Ok(())
    }
}
