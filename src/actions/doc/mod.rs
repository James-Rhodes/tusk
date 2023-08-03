pub mod doc_parser;
pub mod doc_writer;

use std::path::Path;

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;

use crate::config_file_manager::ddl_config::{
    get_matching_file_contents, get_uncommented_file_contents,
};

use self::{doc_parser::FunctionDocParser, doc_writer::FunctionDocWriter};

use super::init::SCHEMA_CONFIG_LOCATION;

#[derive(Debug, Clone)]
struct FunctionFile {
    func_name: String,
    file_path: String,
}

#[derive(Debug, Args)]
pub struct Doc {
    /// Specify that you want to generate docs for functions in all schemas
    #[arg(short, long, exclusive(true))]
    all: bool,

    /// The schemas you want to generate function docs for
    #[clap(num_args = 1.., index=1, required_unless_present="all")]
    schemas: Vec<String>,
}

impl Doc {
    fn get_func_name(file_path: &Path) -> Result<String> {
        let dir = file_path.parent().context(format!(
            "The file directory {:?} should have a parent",
            file_path
        ))?;

        Ok(dir
            .file_name()
            .context(format!(
                "There should be a file name for the given path: {:?}",
                dir
            ))?
            .to_str()
            .context("The file paths provided should be valid UTF-8 Characters")?
            .to_owned())
    }
    fn get_functions_from_schema(schema: &str) -> Result<Vec<FunctionFile>> {
        let mut function_files = vec![];
        let dir_walker = walkdir::WalkDir::new(format!("./schemas/{}/functions", schema))
            .min_depth(2)
            .max_depth(2);
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

            if file_name.ends_with(".sql") {
                let func_name = Self::get_func_name(file_path)?;

                let func_path = file_path
                    .to_str()
                    .context("File path should be convertible into a str")?
                    .to_owned();

                function_files.push(FunctionFile {
                    func_name,
                    file_path: func_path,
                });
            }
        }

        Ok(function_files)
    }

    pub async fn execute(&self) -> Result<()> {
        let mut schemas = get_uncommented_file_contents(SCHEMA_CONFIG_LOCATION)?;

        if !self.all {
            schemas = get_matching_file_contents(schemas.into_iter(), &self.schemas, None)?;
        }

        for schema in &schemas {
            println!("\nBeginning {} schema doc generation:", schema);

            let dir_path = format!("./documentation/{}",schema);
            if std::path::Path::new(&dir_path).exists() {
                // Only clean the directory if it exists already
                std::fs::remove_dir_all(&dir_path)?;
                println!("\t{}: Directory {}", "Cleaned".yellow(), dir_path.magenta());
            }

            let function_files = Self::get_functions_from_schema(schema)?;
            // This is where the multi threadedness will happen
            for ff in function_files {

                let file_contents = tokio::fs::read_to_string(ff.file_path).await?;

                let function_info = FunctionDocParser::new(schema, &ff.func_name, &file_contents)?;

                if let Some(function_info) = function_info {
                    FunctionDocWriter::write_doc_to_file(&function_info).await?;
                }
            }
        }

        Ok(())
    }
}
