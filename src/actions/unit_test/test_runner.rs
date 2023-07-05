use std::collections::HashMap;

use crate::{
    actions::unit_test::test_config_manager::{get_test_config, TestConfig},
    db_manager::error_handling::get_db_error,
};
use anyhow::{bail, Result};
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Column, Executor, PgPool, Row, ValueRef};

use super::test_config_manager::TestSideEffectConfig;

#[derive(Debug, PartialEq, Eq)]
pub enum TestResult {
    Passed {
        test_name: String,
    },
    Failed {
        test_name: String,
        error_message: String,
    },
}

// runs a test given a unit test definition
pub struct TestRunner {
    tests: Vec<TestConfig>,
}

impl TestRunner {
    pub fn new(tests: Vec<TestConfig>) -> Self {
        return Self { tests };
    }

    pub async fn from_file(file_path: &str) -> Result<Self> {
        return Ok(Self::new(get_test_config(file_path).await?));
    }

    pub async fn run_tests(&self, pool: &PgPool) -> Result<Vec<TestResult>> {
        if self.tests.is_empty() {
            // return bail!("There must be at least one test defined per unit test yaml file");
            return Err(anyhow::anyhow!(
                "There must be at least one test defined per unit test yaml file"
            ));
        }
        let mut test_results = Vec::with_capacity(self.tests.len());
        for test in &self.tests {
            let transaction = pool.begin().await?;
            test_results.push(self.run_test(pool, test).await?);
            transaction.rollback().await?;
        }

        return Ok(test_results);
    }

    async fn run_test(&self, pool: &PgPool, test: &TestConfig) -> Result<TestResult> {
        let query: &str = &test.query;

        let func_output_result = match &test.expected_output {
            Some(expected) => {
                self.check_query_results(pool, query, &expected, &test.name, "Query Result")
                    .await?
            }
            None => match pool.execute(query).await {
                Ok(_) => TestResult::Passed {
                    test_name: test.name.clone(),
                },
                Err(e) => TestResult::Failed {
                    test_name: test.name.clone(),
                    error_message: get_db_error(e),
                },
            },
        };

        if let TestResult::Failed {
            test_name: _,
            error_message: _,
        } = func_output_result
        {
            return Ok(func_output_result);
        }

        let func_side_effect_result = match &test.expected_side_effect {
            Some(TestSideEffectConfig {
                table_query,
                expected_query_results,
            }) => {
                self.check_query_results(
                    pool,
                    table_query,
                    expected_query_results,
                    &test.name,
                    "Side Effect",
                )
                .await?
            }
            None => TestResult::Passed {
                test_name: test.name.clone(),
            },
        };

        return Ok(func_side_effect_result);
    }

    async fn check_query_results(
        &self,
        pool: &PgPool,
        query: &str,
        expected_result: &Vec<HashMap<String, String>>,
        test_name: &str,
        test_prefix: &str,
    ) -> Result<TestResult> {
        let mut rows = pool.fetch(query);

        let mut current_row_index = 0;

        while let Some(row) = match rows.try_next().await {
            Ok(row_op) => row_op,
            Err(e) => {
                return Ok(TestResult::Failed {
                    test_name: test_name.to_string(),
                    error_message: get_db_error(e),
                });
            }
        } {
            if current_row_index >= expected_result.len() {
                // The number of returned rows is greater than we expected. Keep going through the
                // stream just to count how many rows were returned
                current_row_index += 1;
                continue;
            }
            let row_map = Self::row_to_map(row)?;

            if row_map != expected_result[current_row_index] {
                return Ok(TestResult::Failed {
                    test_name: test_name.to_string(),
                    error_message: format!(
                        "{}: Returned row was not equal:\nExpected {:?}, Received: {:?}",
                        test_prefix, expected_result[current_row_index], row_map
                    ),
                });
            }
            current_row_index += 1;
        }

        if current_row_index != expected_result.len() {
            // Double check the above
            return Ok(TestResult::Failed{test_name: test_name.to_string(), error_message: format!("{}: The number of returned rows from query: {} was incorrect.\nReceived {} rows, expected {}", test_prefix, query, current_row_index, expected_result.len()) });
        }

        return Ok(TestResult::Passed {
            test_name: test_name.to_string(),
        });
    }

    fn row_to_map(row: PgRow) -> Result<HashMap<String, String>> {
        // Taken from https://stackoverflow.com/questions/72901680/convert-pgrow-value-of-unknown-type-to-a-string
        let mut result = HashMap::new();
        for col in row.columns() {
            let value = row.try_get_raw(col.ordinal())?;
            let value = match value.is_null() {
                true => "NULL".to_string(),
                false => match value.as_str() {
                    Ok(value) => value.to_string(),
                    Err(e) => bail!(e),
                }
                .to_string(),
            };
            result.insert(col.name().to_string(), value);
        }

        return Ok(result);
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::db_manager::DbConnection;

    #[test]
    fn running_tests_without_side_effects_works() {
        let db_connection = tokio_test::block_on(DbConnection::new()).unwrap();

        let test_config_text = r#"
- name: 'Passing Test'
  query: 'SELECT SUM(nums) AS the_sum FROM UNNEST(ARRAY[1,2,3,4,5]) nums;'
  expected_output:
  - the_sum: '15'
- name: 'Failing Test'
  query: 'SELECT SUM(nums) AS the_sum FROM UNNEST(ARRAY[1,2,3,4,5]) nums;'
  expected_output:
  - the_sum: '123'
- name: 'Too few returned rows'
  query: 'SELECT SUM(nums) AS the_sum FROM UNNEST(ARRAY[1,2,3,4,5]) nums;'
  expected_output:
  - the_sum: '15'
  - the_sum: '123'
- name: 'Too many returned rows'
  query: 'SELECT UNNEST(ARRAY[1,2,3]) some_nums;'
  expected_output:
  - some_nums: '1'
- name: 'Checking multiple correct rows work'
  query: 'SELECT UNNEST(ARRAY[1,2,3]) some_nums;'
  expected_output:
  - some_nums: '1'
  - some_nums: '2'
  - some_nums: '3'
- name: 'Checking multiple correct rows and columns work'
  query: 'SELECT UNNEST(ARRAY[1,2]) num1, UNNEST(ARRAY[2,1]) num2;'
  expected_output:
  - num1: '1'
    num2: '2'
  - num1: '2'
    num2: '1'
        "#;

        let test_config: Vec<TestConfig> =
            serde_yaml::from_str(test_config_text).expect("This should never fail");

        let test_runner = TestRunner::new(test_config);

        let pool = db_connection.get_connection_pool();
        let results = tokio_test::block_on(test_runner.run_tests(&pool)).expect("This to not fail");

        assert_eq!(
            results[0],
            TestResult::Passed {
                test_name: "Passing Test".to_string()
            }
        );
        assert_eq!(
            results[1],
            TestResult::Failed {
                test_name: "Failing Test".to_string(),
                error_message: format!(
                    "Query Result: Returned row was not equal:\nExpected {:?}, Received: {:?}",
                    HashMap::from([("the_sum", "123")]),
                    HashMap::from([("the_sum", "15")])
                )
            }
        );
        assert_eq!(results[2], TestResult::Failed{ test_name: "Too few returned rows".to_string(), error_message: format!("Query Result: The number of returned rows from query: {} was incorrect.\nReceived 1 rows, expected 2", "SELECT SUM(nums) AS the_sum FROM UNNEST(ARRAY[1,2,3,4,5]) nums;") });

        assert_eq!(results[3], TestResult::Failed{test_name: "Too many returned rows".to_string(), error_message: format!("Query Result: The number of returned rows from query: {} was incorrect.\nReceived 3 rows, expected 1", "SELECT UNNEST(ARRAY[1,2,3]) some_nums;") });

        assert_eq!(
            results[4],
            TestResult::Passed {
                test_name: "Checking multiple correct rows work".to_string()
            }
        );

        assert_eq!(
            results[5],
            TestResult::Passed {
                test_name: "Checking multiple correct rows and columns work".to_string()
            }
        );
    }

    #[test]
    fn running_tests_with_side_effects_works() {
        let db_connection = tokio_test::block_on(DbConnection::new()).unwrap();
        let pool = db_connection.get_connection_pool();
        tokio_test::block_on(pool.execute("DROP TABLE IF EXISTS public.tusk_test;")).unwrap();
        tokio_test::block_on(pool.execute("CREATE TABLE public.tusk_test(name TEXT, num BIGINT);"))
            .unwrap();

        let test_config_text = r#"

- name: Passing Test
  query: INSERT INTO public.tusk_test(name) VALUES ('Foo'), ('Foo');
  expected_output:
  expected_side_effect:
    table_query: SELECT COUNT(*) AS cnt FROM public.tusk_test WHERE name = 'Foo';
    expected_query_results:
    - cnt: 2
- name: Too few returned rows
  query: INSERT INTO public.tusk_test(name) VALUES ('Bar'), ('Bar');
  expected_output:
  expected_side_effect:
    table_query: SELECT name FROM public.tusk_test WHERE name = 'Bar';
    expected_query_results:
    - name: Bar
    - name: Bar
    - name: Bar
- name: Too many returned rows
  query: INSERT INTO public.tusk_test(name) VALUES ('Baz'), ('Baz');
  expected_output:
  expected_side_effect:
    table_query: SELECT name FROM public.tusk_test WHERE name = 'Baz';
    expected_query_results:
    - name: Baz
- name: Multiple Correct Rows
  query: INSERT INTO public.tusk_test(name) VALUES ('Henry'), ('Henry');
  expected_output:
  expected_side_effect:
    table_query: SELECT name FROM public.tusk_test WHERE name = 'Henry';
    expected_query_results:
    - name: Henry
    - name: Henry
- name: Multiple Correct Rows and Columns
  query: INSERT INTO public.tusk_test(name, num) VALUES ('George', 17), ('George', 17);
  expected_output:
  expected_side_effect:
    table_query: SELECT name, num FROM public.tusk_test WHERE name = 'George';
    expected_query_results:
    - name: George
      num: 17
    - name: George
      num: 17
        "#;

        let test_config: Vec<TestConfig> =
            serde_yaml::from_str(test_config_text).expect("This should never fail");

        let test_runner = TestRunner::new(test_config);

        let results = tokio_test::block_on(test_runner.run_tests(&pool)).expect("This to not fail");

        assert_eq!(
            results[0],
            TestResult::Passed {
                test_name: "Passing Test".to_string()
            }
        );
        assert_eq!(results[1], TestResult::Failed{test_name: "Too few returned rows".to_string(), error_message: format!("Side Effect: The number of returned rows from query: {} was incorrect.\nReceived 2 rows, expected 3", "SELECT name FROM public.tusk_test WHERE name = 'Bar';")});

        assert_eq!(results[2], TestResult::Failed{test_name: "Too many returned rows".to_string(), error_message: format!("Side Effect: The number of returned rows from query: {} was incorrect.\nReceived 2 rows, expected 1", "SELECT name FROM public.tusk_test WHERE name = 'Baz';")});

        assert_eq!(
            results[3],
            TestResult::Passed {
                test_name: "Multiple Correct Rows".to_string()
            }
        );

        assert_eq!(
            results[4],
            TestResult::Passed {
                test_name: "Multiple Correct Rows and Columns".to_string()
            }
        );

        tokio_test::block_on(pool.execute("DROP TABLE IF EXISTS public.tusk_test;")).unwrap();
    }
}
