use std::collections::HashMap;

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use sqlx::{Acquire, Postgres};
use walkdir;

use crate::{
    actions::{init::SCHEMA_CONFIG_LOCATION, unit_test::UnitTest},
    config_file_manager::{
        ddl_config::{
            get_commented_file_contents, get_matching_file_contents, get_uncommented_file_contents,
        },
        user_config::UserConfig,
    },
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
    #[arg(short, long, conflicts_with("functions"))]
    all: bool,

    /// Force unit tests to be run, rolling back the push of functions if any unit tests fail
    #[arg(long)]
    #[clap(conflicts_with = "no_test")]
    test: bool,

    /// Don't run unit tests after the pushing of functions
    #[arg(long)]
    #[clap(conflicts_with = "test")]
    no_test: bool,

    /// Adding this flag will give a preview of what is going to be pushed and allow the user to accept or
    /// deny the items to be pushed.
    #[arg(long)]
    confirm: bool,

    #[clap(skip)]
    user_config_confirm_before_push: bool,
}

impl Push {
    // Get all locally defined functions within the directory schema_dir
    fn get_local_funcs(&self, schema: &str) -> Result<HashMap<String, Vec<String>>> {
        let mut func_paths: HashMap<String, Vec<String>> = HashMap::new();

        let function_dir = &format!("./schemas/{}/functions", schema);
        let function_dir = std::path::Path::new(function_dir);

        if !function_dir.exists() {
            return Ok(HashMap::new());
        }

        let dir_walker = walkdir::WalkDir::new(function_dir).max_depth(2);
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

        Ok(func_paths)
    }

    async fn push_func<'c, C>(
        &self,
        conn: C,
        func_name: &str,
        func_paths: &Vec<String>,
    ) -> Result<()>
    where
        C: Acquire<'c, Database = Postgres>,
    {
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
                    return Err(anyhow::anyhow!("All functions have been rolled back. Please fix the error within the function defined at: \n\t'{func_path}'"));
                }
            };
        }

        Ok(())
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();
        let mut transaction = pool.begin().await?;

        let schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        self.user_config_confirm_before_push =
            UserConfig::get_global()?.push_options.confirm_before_push;

        println!("\nBeginning Push:");

        for schema in schemas {
            let local_func_paths = self.get_local_funcs(&schema)?;
            let commented_funcs = get_commented_file_contents(&format!(
                "./.tusk/config/schemas/{}/functions_to_include.conf",
                schema
            ))?;

            // Remove all local funcs that are commented in the config file
            let local_func_paths = local_func_paths
                .into_iter()
                .filter(|(key, _)| !commented_funcs.contains(key))
                .collect::<HashMap<String, Vec<String>>>();

            let function_path_map = match self.all {
                true => local_func_paths,
                false => {
                    let matching_local_funcs = get_matching_file_contents(
                        local_func_paths.keys(),
                        &self.functions,
                        Some(&schema),
                    )?;

                    local_func_paths
                        .clone()
                        .into_iter()
                        .filter(|(func_name, _)| matching_local_funcs.contains(&func_name))
                        .collect()
                }
            };

            if !function_path_map.is_empty() {
                println!("\nBeginning {} schema push:", schema);
            }

            if (self.user_config_confirm_before_push || self.confirm)
                && !function_path_map.is_empty()
                && !UserConfig::user_confirmed(&schema, function_path_map.keys())?
            {
                anyhow::bail!("The items were rejected by the user. Please filter appropriately on the next run")
            }
            for (func_name, func_paths) in function_path_map.iter() {
                self.push_func(&mut *transaction, func_name, func_paths)
                    .await?;
            }
        }

        let should_unit_test = self.test || UserConfig::get_global()?.push_options.test_after_push;

        if should_unit_test && !self.no_test {
            // Run the unit tests
            let test_results =
                UnitTest::run_unit_tests(&mut *transaction, &self.functions, self.all).await?;
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
