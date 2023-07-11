pub mod test_config_manager;
pub mod test_runner;

use std::{collections::HashMap, path::Path};

use anyhow::{bail, Context, Result};
use clap::Args;
use colored::Colorize;
use sqlx::{Acquire, Postgres};

use crate::{
    actions::{init::SCHEMA_CONFIG_LOCATION, unit_test::test_runner::TestRunner},
    config_file_manager::ddl_config::{
        get_commented_file_contents, get_matching_file_contents, get_uncommented_file_contents,
    },
    db_manager,
};

#[derive(Default, Debug)]
pub struct TestStats {
    pub num_passed: u32,
    pub num_failed: u32,
}

impl std::ops::AddAssign for TestStats {
    fn add_assign(&mut self, rhs: Self) {
        self.num_passed += rhs.num_passed;
        self.num_failed += rhs.num_failed;
    }
}

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
    fn get_func_name(file_path: &Path) -> Result<String> {
        let mut dir = file_path.parent().context(format!(
            "The file directory {:?} should have a parent",
            file_path
        ))?;

        let mut current_name = dir
            .file_name()
            .context(format!(
                "There should be a file name for the given path: {:?}",
                dir
            ))?
            .to_str()
            .context("The file paths provided should be valid UTF-8 Characters")?;

        while let Some(child_dir) = dir.parent() {
            // walk back out from the unit test path until we reach the function name which is the
            // name of the folder immediately after functions eg. functions/{some func name}
            let next_name = child_dir
                .file_name()
                .context(format!(
                    "There should be a file name for the given path: {:?}",
                    child_dir
                ))?
                .to_str()
                .context("The file paths provided should be valid UTF-8 Characters")?;

            if next_name == "functions" {
                return Ok(current_name.to_string());
            }

            current_name = next_name;
            dir = child_dir;
        }
        bail!(
            "Could not find the given function name for path: {:?}",
            file_path
        );
    }

    // Get all locally defined functions and their unit test paths. First element of tuple is name
    // of the function second is a map from the name to a vector of associated paths
    fn get_func_unit_test_paths(schema: &str) -> Result<HashMap<String, Vec<String>>> {
        let mut unit_test_paths: HashMap<String, Vec<String>> = HashMap::new();

        let dir_walker =
            walkdir::WalkDir::new(format!("./schemas/{}/functions", schema)).min_depth(3);
        for dir in dir_walker
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| !e.file_type().is_dir())
        {
            let file_name = dir
                .file_name()
                .to_str()
                .expect("File path should be able to be converted to a str");
            let file_path = dir.path();

            if file_name.ends_with(".yaml") || file_name.ends_with(".yml") {
                let func_name = Self::get_func_name(file_path)?;

                let unit_test_path = file_path
                    .to_str()
                    .context("File path should be convertible into a str")?
                    .to_owned();

                let unit_test_path_list = unit_test_paths.entry(func_name).or_insert(vec![]);
                unit_test_path_list.push(unit_test_path);
            }
        }

        Ok(unit_test_paths)
    }

    async fn run_function_unit_test<'a, C>(conn: C, file_paths: &Vec<String>) -> Result<TestStats>
    where
        C: Acquire<'a, Database = Postgres>,
    {
        let mut conn = conn.acquire().await?;

        let mut test_stats = TestStats::default();
        for fp in file_paths {
            let test_runner = TestRunner::from_file(fp).await?;
            let test_results = test_runner.run_tests(&mut *conn).await?;
            for test_result in test_results {
                // print the messages about pass or fail. add to the tally for passed vs failed
                match test_result {
                    test_runner::TestResult::Passed { test_name } => {
                        println!(
                            "\t{}::{} - {}",
                            fp.magenta(),
                            test_name.bold(),
                            "Passed".green()
                        );
                        test_stats.num_passed += 1;
                    }
                    test_runner::TestResult::Failed {
                        test_name,
                        error_message,
                    } => {
                        println!(
                            "\t{}::{} - {}",
                            fp.magenta(),
                            test_name.bold(),
                            "Failed".red()
                        );
                        println!("\t\t{}", error_message.replace('\n', "\n\t\t"));
                        test_stats.num_failed += 1;
                    }
                }
            }
        }
        Ok(test_stats)
    }

    pub async fn run_unit_tests<'c, C>(
        conn: C,
        functions: &[String],
        run_all: bool,
    ) -> Result<TestStats>
    where
        C: Acquire<'c, Database = Postgres>,
    {
        let mut conn = conn.acquire().await?;
        let mut total_stats = TestStats::default();

        let schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        println!("\nBeginning Unit Tests:");

        for schema in schemas {
            let unit_test_paths = Self::get_func_unit_test_paths(&schema)?;
            let commented_funcs = get_commented_file_contents(&format!(
                "./.tusk/config/schemas/{}/functions_to_include.conf",
                schema
            ))?;

            // Remove all local funcs that are commented in the config file
            let unit_test_paths = unit_test_paths
                .into_iter()
                .filter(|(item, _)| !commented_funcs.contains(item))
                .collect::<HashMap<String, Vec<String>>>();

            if run_all {
                // If all is specified then just run all the local functions unit tests that aren't commented
                if !unit_test_paths.is_empty() {
                    println!("\nBeginning {} schema unit tests:", schema);
                }
                for (_, test_paths) in unit_test_paths.iter() {
                    total_stats += Self::run_function_unit_test(&mut *conn, test_paths).await?;
                }
            } else {
                // Get the functions that match the patterns passed in
                let matching_local_funcs =
                    get_matching_file_contents(unit_test_paths.keys(), functions, Some(&schema))?;

                if !matching_local_funcs.is_empty() {
                    println!("\nBeginning {} schema unit tests:", schema);
                }

                for func in matching_local_funcs {
                    total_stats += Self::run_function_unit_test(
                        &mut *conn,
                        unit_test_paths
                            .get(func)
                            .context("The function path should match a function")?,
                    )
                    .await?;
                }
            }
        }
        Ok(total_stats)
    }
    pub async fn execute(&self) -> anyhow::Result<()> {
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();

        Self::run_unit_tests(pool, &self.functions, self.all).await?;

        Ok(())
    }
}
