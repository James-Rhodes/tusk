use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use sqlx::PgPool;

use crate::{
    actions::init::SCHEMA_CONFIG_LOCATION,
    config_file_manager::{
        get_commented_file_contents, get_matching_file_contents, get_uncommented_file_contents,
    }, db_manager,
};

use super::Action;

#[derive(Debug, Args)]
pub struct Push {
    /// The functions to push to the database. Specify the schema as my_schema.func or my_schema.% to
    /// push all of the functions within my_schema
    #[clap(num_args = 0.., trailing_var_arg=true, index=1, required_unless_present="all")]
    // This is how you allow it to be a
    // positional argument rather than a flagged argument
    functions: Vec<String>,

    /// Push all of the functions from all schemas to the database
    #[arg(short, long)]
    all: bool,
}

impl Push {
    // Get all locally defined functions within the directory schema_dir
    fn get_local_funcs(&self, schema: &str) -> Result<Vec<String>> {
        todo!();
    }

    async fn push_func(&self,pool: &PgPool, schema: &str, func: &str) -> Result<()> {
        todo!();
    }
}

#[async_trait]
impl Action for Push {
    async fn execute(&self) -> anyhow::Result<()> {

        println!("Funcs: {:?}", self.functions);

        let pool = db_manager::get_db_connection().await?;

        let schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        for schema in schemas {
            let local_funcs = self.get_local_funcs(&schema)?;
            let commented_funcs = get_commented_file_contents(&format!(
                "./.tusk/config/schemas/{}/functions_to_include.conf",
                schema
            ))?;

            // Remove all local funcs that are commented in the config file
            let local_funcs = local_funcs
                .into_iter()
                .filter(|item| !commented_funcs.contains(item))
                .collect::<Vec<String>>();

            if self.all {
                // If all is specified then just sync all the local functions that aren't commented
                for func in local_funcs.iter() {
                    self.push_func(&pool, &schema, &func).await?;
                }
            } else {
                // Get the functions that match the patterns passed in
                let matching_local_funcs =
                    get_matching_file_contents(&local_funcs, &self.functions, Some(&schema))?;

                for func in matching_local_funcs {
                    self.push_func(&pool, &schema, &func).await?;
                }
            }
        }

        return Ok(());
    }
}
