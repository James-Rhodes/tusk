pub mod test_config_manager;
pub mod test_runner;

use async_trait::async_trait;
use clap::Args;
// use colored::Colorize;

use crate::actions::Action;

#[derive(Debug, Args)]
pub struct UnitTest { 
    /// The functions to unit test. Specify the schema as my_schema.func or my_schema.% to
    /// test all of the functions within my_schema.
    /// Please note that this will run the functions in a transaction which will be rolled back at
    /// the completion of the unit tests. This does not guarentee no side effects if your function
    /// or procedure contains a COMMIT 
    #[clap(num_args = 0.., trailing_var_arg=true, index=1, required_unless_present="all")]
    // This is how you allow it to be a
    // positional argument rather than a flagged argument
    functions: Vec<String>,

    /// test all of the functions that specify unit tests from all schemas
    #[arg(short, long)]
    all: bool,
}

impl UnitTest {
}

#[async_trait]
impl Action for UnitTest {
    async fn execute(&self) -> anyhow::Result<()> {
        todo!();
    }
}
