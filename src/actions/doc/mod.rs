use anyhow::Result;
use clap::Args;

#[derive(Debug, Args)]
pub struct Doc {
    /// Specify that you want to generate docs for functions in all schemas
    #[arg(short, long, exclusive(true))]
    all: bool,

    /// The schemas you want to generate function docs for
    #[clap(num_args = 0.., index=1, required_unless_present="all")]
    schemas: Vec<String>,
}

impl Doc {
    pub async fn execute(&self) -> Result<()> {
        println!("This will eventually build the docs once fully fleshed out");
        Ok(())
    }

}
