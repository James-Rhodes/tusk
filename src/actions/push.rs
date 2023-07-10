use std::collections::HashMap;

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use sqlx::{Acquire, Postgres};
use walkdir;

use crate::{
    actions::{init::SCHEMA_CONFIG_LOCATION, unit_test::UnitTest},
    config_file_manager::{ddl_config::{
        get_commented_file_contents, get_matching_file_contents, get_uncommented_file_contents,
    }, user_config::UserConfig},
    db_manager,
};

#[derive(Debug, Args)]
pub struct Push {
    /// The functions to push to the database. Specify the schema as my_schema.func or my_schema.% to
    /// push all of the functions within my_schema
    #[clap(num_args = 0.., index=1, required_unless_present="all")]
    // This is how you allow it to be a
    // positional argument rather than a flagged argument
    functions: Vec<String>,

    /// Push all of the functions from all schemas to the database
    #[arg(short, long)]
    all: bool,

    /// Force unit tests to be run, rolling back the push of functions if any unit tests fail
    #[arg(long)]
    #[clap(conflicts_with="no_test")]
    test: bool,

    /// Don't run unit tests after the pushing of functions
    #[arg(long)]
    #[clap(conflicts_with="test")]
    no_test: bool,
}

impl Push {
    // Get all locally defined functions within the directory schema_dir
    fn get_local_funcs(&self, schema: &str) -> Result<(Vec<String>, HashMap<String, Vec<String>>)> {

        let mut func_paths: HashMap<String, Vec<String>> = HashMap::new();

        let dir_walker =
            walkdir::WalkDir::new(format!("./schemas/{}/functions", schema)).max_depth(2);
        for dir in dir_walker.into_iter() {
            let dir = dir?;
            let file_name = dir
                .file_name()
                .to_str()
                .expect("File path should be able to be converted to a str");
            let file_path = dir.path();
            if file_path.is_file() && file_name.ends_with(".sql") {
                let func_name = file_path
                    .parent()
                    .expect("File should have a parent dir")
                    .file_name()
                    .expect("The folder should have a name")
                    .to_str()
                    .expect("File name should be convertible into a str")
                    .to_owned();

                let func_path = file_path
                    .to_str()
                    .expect("File path should be convertible into a str")
                    .to_owned();

                let func_path_list = func_paths.entry(func_name).or_insert(vec![]);
                func_path_list.push(func_path);
            }
        }

        return Ok((func_paths.keys().map(|val| val.to_string()).collect(), func_paths));
    }

    async fn push_func<'c, C>(
        &self,
        conn: C,
        func_name: &str,
        func_paths: &Vec<String>,
    ) -> Result<()> 
    where C: Acquire<'c, Database = Postgres> {
        let mut conn = conn.acquire().await?;
        for func_path in func_paths {
            let file_contents = std::fs::read_to_string(func_path)?;
            match sqlx::query(&file_contents).execute(&mut *conn).await {
                Ok(_) => println!(
                    "\t{}: {} {}",
                    func_name.bold().magenta(),
                    func_path,
                    "Success".green()
                ),
                Err(e) => {
                    println!(
                        "\t{}: {} {}",
                        func_name.bold().magenta(),
                        func_path,
                        "Failed".red()
                    );
                    let error_text = db_manager::error_handling::get_db_error(e);
                    println!("\t\t{}", error_text);
                }
            };
        }

        Ok(())
    }

    pub async fn execute(&self) -> anyhow::Result<()> {
        // TODO: Refactor this to run all of the pushing within a transaction so it can be rolled
        // back
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();
        let mut transaction = pool.begin().await?;

        let schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        println!("\nBeginning Push:");

        for schema in schemas {
            let (local_funcs, local_func_paths) = self.get_local_funcs(&schema)?;
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
                // If all is specified then just pull all the local functions that aren't commented
                if !local_funcs.is_empty() {
                    println!("\nBeginning {} schema push:", schema);
                }
                for func in local_funcs.iter() {
                    self.push_func(
                        &mut *transaction,
                        func,
                        local_func_paths
                            .get(func)
                            .expect("The function path should match a function"),
                    )
                    .await?;
                }
            } else {
                // Get the functions that match the patterns passed in
                let matching_local_funcs =
                get_matching_file_contents(&local_funcs, &self.functions, Some(&schema))?;

                if !matching_local_funcs.is_empty() {
                    println!("\nBeginning {} schema push:", schema);
                }

                for func in matching_local_funcs {
                    self.push_func(
                        &mut *transaction,
                        func,
                        local_func_paths
                            .get(func)
                            .expect("The function path should match a function"),
                    )
                    .await?;
                }
            }
        }
        let should_unit_test = self.test || UserConfig::get_global()?.push_options.test_after_push;

        if should_unit_test && !self.no_test {
            // Run the unit tests
            let test_results = UnitTest::run_unit_tests(&mut *transaction, &self.functions, self.all).await?;
            if test_results.num_failed != 0 {
                println!("{}: Due to unit test failure, all functions have been rolled back to their original state.", "Error".red());
                transaction.rollback().await?;
                return Ok(());
            }
        }

        transaction.commit().await?;

        Ok(())
    }
}
