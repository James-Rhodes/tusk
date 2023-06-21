use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use clap::Args;
use colored::Colorize;
use sqlx::{postgres::PgDatabaseError, PgPool};
use walkdir;

use crate::{
    actions::init::SCHEMA_CONFIG_LOCATION,
    config_file_manager::ddl_config::{
        get_commented_file_contents, get_matching_file_contents, get_uncommented_file_contents,
    },
    db_manager,
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
    fn get_local_funcs(&self, schema: &str) -> Result<(Vec<String>, HashMap<String, Vec<String>>)> {
        let mut func_names = vec![];
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

                func_names.push(func_name.clone());

                let func_path_list = func_paths.entry(func_name).or_insert(vec![]);
                func_path_list.push(func_path);
            }
        }

        return Ok((func_names, func_paths));
    }

    async fn push_func(
        &self,
        pool: &PgPool,
        func_name: &str,
        func_paths: &Vec<String>,
    ) -> Result<()> {
        for func_path in func_paths {
            let file_contents = std::fs::read_to_string(func_path)?;
            match sqlx::query(&file_contents).execute(pool).await {
                Ok(_) => println!(
                    "\t-{}: {} {}",
                    func_name.bold().magenta(),
                    func_path,
                    "Success".green()
                ),
                Err(e) => {
                    println!(
                        "\t-{}: {} {}",
                        func_name.bold().magenta(),
                        func_path,
                        "Failed".red()
                    );
                    match e {
                        sqlx::Error::Database(e) => match e.try_downcast::<PgDatabaseError>() {
                            Ok(e) => {
                                let message = e.message();

                                let detail = e.detail().unwrap_or_default();
                                let hint = e.hint().unwrap_or_default();

                                let pos = match e.position() {
                                    Some(sqlx::postgres::PgErrorPosition::Original(position)) => {
                                        position.to_string()
                                    }
                                    Some(sqlx::postgres::PgErrorPosition::Internal {
                                        position,
                                        query,
                                    }) => format!(
                                        "{} for query {}",
                                        position.to_string(),
                                        query.to_string()
                                    ),
                                    None => String::from(""),
                                };
                                println!(
                                    "\t\t{}: {}, Position: {}, Detail: {}, Hint: {}",
                                    "Error".red(),
                                    message,
                                    pos,
                                    detail,
                                    hint
                                )
                            }
                            Err(e) => println!("\t\t{}: {}", "Error".red(), e.to_string()),
                        },
                        _ => println!("\t\t{}: An unexpected error occured", "Error".red()),
                    }
                }
            };
        }

        return Ok(());
    }
}

#[async_trait]
impl Action for Push {
    async fn execute(&self) -> anyhow::Result<()> {
        let connection = db_manager::DbConnection::new().await?;
        let pool = connection.get_connection_pool();

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
                .filter(|item| !commented_funcs.contains(&item))
                .collect::<Vec<String>>();

            if self.all {
                // If all is specified then just pull all the local functions that aren't commented
                if !local_funcs.is_empty() {
                    println!("\nBeginning {} schema push:", schema);
                }
                for func in local_funcs.iter() {
                    self.push_func(
                        pool,
                        &func,
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
                        pool,
                        &func,
                        local_func_paths
                            .get(func)
                            .expect("The function path should match a function"),
                    )
                    .await?;
                }
            }
        }

        return Ok(());
    }
}
