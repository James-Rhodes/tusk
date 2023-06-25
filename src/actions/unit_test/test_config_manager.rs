use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

// gets the unit tests from the config file
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TestSideEffectConfig {
    table_query: String,
    expected_query_results: Vec<HashMap<String, String>>,
}
//
// The definition of a test from the json files
#[derive(Debug, Serialize, Deserialize)]
pub struct TestConfig {
    pub name: String,
    pub query: String,
    pub expected_output: Option<Vec<HashMap<String, String>>>,
    pub expected_side_effect: Option<TestSideEffectConfig>,
}

// Manage the config files such as getting tests etc
// struct TestConfigManager {}

// impl TestConfigManager {
async fn get_test_config(file_path: &str) -> Result<Vec<TestConfig>> {
    let yaml_text = tokio::fs::read_to_string(file_path).await?;

    let test_config: Vec<TestConfig> = serde_yaml::from_str(&yaml_text)?;

    return Ok(test_config);
}
// }

#[cfg(test)]
mod tests {
    use super::*;

    mod test_config_tests {
        use super::*;
        #[test]
        fn deserialize_works() {
            let example_json = r#"
- name: The test name
  query: SOME QUERY
  expected_output:
  - col1: '1'
    col2: '2'
  - col1: '3'
    col2: '4'
  expected_side_effect:
    table_query: SOME OTHER QUERY
    expected_query_results:
    - col1: '1'
      col2: '2'
    - col1: '3'
      col2: '4'
            "#;

            let config: Vec<TestConfig> = serde_yaml::from_str(example_json).unwrap();

            assert!(config.len() == 1);

            assert!(config[0].name == "The test name");
            assert!(config[0].query == "SOME QUERY");
            assert!(
                config[0].expected_output
                    == Some(vec![
                        HashMap::from([
                            ("col1".to_string(), "1".to_string()),
                            ("col2".to_string(), "2".to_string())
                        ]),
                        HashMap::from([
                            ("col1".to_string(), "3".to_string()),
                            ("col2".to_string(), "4".to_string())
                        ])
                    ])
            );

            assert!(
                config[0].expected_side_effect
                    == Some(TestSideEffectConfig {
                        table_query: "SOME OTHER QUERY".to_string(),
                        expected_query_results: vec![
                            HashMap::from([
                                ("col1".to_string(), "1".to_string()),
                                ("col2".to_string(), "2".to_string())
                            ]),
                            HashMap::from([
                                ("col1".to_string(), "3".to_string()),
                                ("col2".to_string(), "4".to_string())
                            ])
                        ]
                    })
            );
        }
    }
}
