use std::collections::HashMap;

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};


static USER_CONFIG: OnceLock<UserConfig> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchOptions {
    pub new_items_commented: HashMap<String, bool>,
    pub delete_items_from_config: bool
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullOptions{
    pub clean_ddl_before_pulling: bool,
    pub pg_dump_additional_args: Vec<String>
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushOptions{
    pub test_after_push: bool
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserConfig {
    pub fetch_options: FetchOptions,
    pub pull_options: PullOptions,
    pub push_options: PushOptions
}

impl UserConfig{
    pub fn init(file_path: &str) -> Result<()> {

        let user_config = serde_yaml::from_str(&std::fs::read_to_string(file_path)?)?;
        USER_CONFIG.set(user_config).expect("This should only be called by one thread in this application");

        return Ok(());
    }

    pub fn get_global() -> Result<&'static UserConfig> {
        return USER_CONFIG.get().context("User Config must be set before this variable can be used");
    }
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn config_parsing_works(){
        let test_yaml = r#"
fetch_options:
    new_items_commented:
      schemas: true
      functions: false
      table_ddl: false
      table_data: true
      views: false
      data_types: false
    delete_items_from_config: true

pull_options:
    clean_ddl_before_pulling: true
    pg_dump_additional_args:
      - "--no-privileges"
      - "--no-tablespaces"
push_options:
    test_after_push: true
        "#;


        let parsed: UserConfig = serde_yaml::from_str(&test_yaml).expect("This should never fail");

        let expected = UserConfig{
            fetch_options: FetchOptions{
                new_items_commented: HashMap::from([
                    ("functions".to_string(), false), 
                    ("table_ddl".to_string(), false), 
                    ("table_data".to_string(), true), 
                    ("views".to_string(), false),
                    ("schemas".to_string(), true),
                    ("data_types".to_string(), false)
                ]),
                delete_items_from_config: true
            },
            pull_options: PullOptions{
                clean_ddl_before_pulling: true,
                pg_dump_additional_args: vec!["--no-privileges".to_string(), "--no-tablespaces".to_string()],
            },
            push_options: PushOptions{
                test_after_push: true
            }
        };

        assert!(parsed == expected);
    }
}


