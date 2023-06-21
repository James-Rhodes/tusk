use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FetchOptions {
    new_items_commented: HashMap<String, bool>,
    delete_items_from_config: bool
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullOptions{
    clean_ddl_before_pulling: bool,
    pg_dump_additional_args: Vec<String>
}

// #[derive(Debug, Serialize, Deserialize)]
// pub struct PushOptions{}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserConfig {
    fetch_options: FetchOptions,
    pull_options: PullOptions,
    // _push_options: PushOptions
}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn config_parsing_works(){
        let test_yaml = r#"
fetch_options:
    new_items_commented:
      functions: false
      table_ddl: false
      table_data: true
      views: false
    delete_items_from_config: true

pull_options:
    clean_ddl_before_pulling: true
    pg_dump_additional_args:
      - "--no-privileges"
      - "--no-tablespaces"
        "#;


        let parsed: UserConfig = serde_yaml::from_str(&test_yaml).expect("This should never fail");

        let expected = UserConfig{
            fetch_options: FetchOptions{
                new_items_commented: HashMap::from([
                    ("functions".to_string(), false), 
                    ("table_ddl".to_string(), false), 
                    ("table_data".to_string(), true), 
                    ("views".to_string(), false)
                ]),
                delete_items_from_config: true
            },
            pull_options: PullOptions{
                clean_ddl_before_pulling: true,
                pg_dump_additional_args: vec!["--no-privileges".to_string(), "--no-tablespaces".to_string()],
            }
        };

        assert!(parsed == expected);
    }
}


