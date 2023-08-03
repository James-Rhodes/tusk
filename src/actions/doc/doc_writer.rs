use anyhow::{Result, Context};
use colored::Colorize;
use tokio::io::AsyncWriteExt;

use super::doc_parser::FunctionDocParser;

pub struct FunctionDocWriter {}

impl FunctionDocWriter {
    pub async fn write_doc_to_file(function_info: &FunctionDocParser<'_>) -> Result<()> {
        // If the function docs already exist (in the case of an overload)
        // then just append to the fil;e rather than creating from scrathc

        let file_name = &format!(
            "./documentation/{}/{}.md",
            function_info.schema, function_info.function_name
        );
        let file_path = std::path::Path::new(&file_name);

        let mut file_content = String::new();

        if !file_path.exists() {
            // Append heading
            file_content.push_str(&format!("# {}\n\n", function_info.function_name));
        }

        tokio::fs::create_dir_all(file_path.parent().context("This file should have a parent directory")?).await?;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(file_path)
            .await?;


        file_content.push_str(&format!("## {}\n", function_info.function_full_name));

        if let Some(author) = function_info.author {
            file_content.push_str(&format!("- Author: {}\n", author));
        }

        if let Some(date) = function_info.date {
            file_content.push_str(&format!("- Date: {}\n\n", date));
        }

        file_content.push_str(&format!(
            "### Description \n{}\n\n",
            function_info.description
        ));

        if let Some(params) = &function_info.params {
            file_content.push_str("### Arguments\n\n");

            // Get the text width required
            let mut max_name_width = 4; // "name".len()
            let mut max_param_type_width = 4; // "type".len()
            let mut max_description_width = 11; // "description".len()
            for param in params {
                max_name_width = max_name_width.max(param.name.len());
                max_param_type_width = max_param_type_width.max(param.param_type.len());
                max_description_width = max_description_width.max(
                    param
                        .description
                        .unwrap_or_default()
                        .replace("\r\n", "")
                        .replace('\n', "")
                        .len(),
                );
            }

            file_content.push_str(&format!(
                "| {:max_name_width$} | {:max_param_type_width$} | {:max_description_width$} |\n",
                "Name", "Type", "Description"
            ));

            file_content.push_str(&format!(
                "| {:-<max_name_width$} | {:-<max_param_type_width$} | {:-<max_description_width$} |\n",
                "", "", ""
            ));

            for param in params {
                file_content.push_str(&format!(
                    "| {:<max_name_width$} | {:<max_param_type_width$} | {:<max_description_width$} |\n",
                    param.name, param.param_type, param.description.unwrap_or_default().replace("\r\n", "").replace('\n', "")
                ));
            }
        }

        if let Some(return_val) = &function_info.returns {
            file_content.push_str("\n### Return Type\n\n");

            // Get the text width required
            let mut max_type_width = 4; // "Type".len()
            let mut max_description_width = 11; // "Description".len()

            max_type_width = max_type_width.max(return_val.return_type.len());
            max_description_width = max_description_width.max(
                return_val
                    .description
                    .unwrap_or_default()
                    .replace("\r\n", "")
                    .replace('\n', "")
                    .len(),
            );

            file_content.push_str(&format!(
                "| {:max_type_width$} | {:max_description_width$} |\n",
                "Type", "Description"
            ));

            file_content.push_str(&format!(
                "| {:-<max_type_width$} | {:-<max_description_width$} |\n",
                "", ""
            ));

            file_content.push_str(&format!(
                "| {:max_type_width$} | {:max_description_width$} |\n\n",
                return_val.return_type,
                return_val
                    .description
                    .unwrap_or_default()
                    .replace("\r\n", "")
                    .replace('\n', "")
            ));
        }

        if let Some(example) = function_info.example {
            file_content.push_str("### Example\n\n");
            file_content.push_str(&format!("```sql\n{}\n```\n\n", example));
        }

        file.write_all(file_content.as_bytes()).await?;
        println!("\t{} Docs Generated", function_info.function_name.bold().magenta());
        Ok(())
    }
}

// Example:
//
//    # Concatenating
//
//    ## concatenating(var1 text, var2 text)
//    - Author: Homer Simpson
//    - Date: 01/02/1234
//
//    ### Description
//
//    This is the function description
//
//    ### Arguments
//    | Name | Type | Description |
//    | --- | --- | ---|
//    | var1 | TEXT | The first part of the output |
//    | var2 | TEXT | The second part of the output |
//
//
//    ### Return Value
//
//    | Type | Description |
//    | --- | --- |
//    | TEXT | var1 and var2 concatenated together |
//
//    ### Example
//    ```sql
//    SELECT public.concatenating('Hello ', 'World');
