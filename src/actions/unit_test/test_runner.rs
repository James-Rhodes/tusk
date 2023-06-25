use std::collections::HashMap;

use crate::{actions::unit_test::test_config_manager::TestConfig, db_manager};
use anyhow::{Result, bail};
use futures::TryStreamExt;
use sqlx::{postgres::PgRow, Column, Executor, PgPool, Row, ValueRef};

use super::test_config_manager::TestSideEffectConfig;

pub enum TestResult {
    Passed,
    Failed(String),
}

// runs a test given a unit test definition
pub struct TestRunner {
    function_path: String,
    tests: Vec<TestConfig>,
}

impl TestRunner {
    fn new(function_path: String, tests: Vec<TestConfig>) -> Self {
        return Self {
            function_path,
            tests,
        };
    }

    async fn run_tests(&self, pool: &PgPool) -> Result<Vec<TestResult>> {
        if self.tests.is_empty() {
            // return bail!("There must be at least one test defined per unit test yaml file");
            return Err(anyhow::anyhow!("There must be at least one test defined per unit test yaml file"));
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
        // let query = sqlx::query(&test.query);
        // let query: &str = &test.query;
        // let mut rows = pool.fetch(query);
        //
        // while let Some(row) = rows.try_next().await? {
        //     // map the row into a user-defined domain type
        //     let email: &str = row.try_get("email")?;
        //     // let row_map: HashMap<&str, &str> = row.try_into()?;
        // }

        let query: &str = &test.query;

        let func_output_result = match &test.expected_output {
            Some(expected) => self.check_function_results(pool, query, &expected).await?,
            None => {
                pool.execute(query).await?;
                TestResult::Passed
            }
        };

        if let TestResult::Failed(_) = func_output_result {
            return Ok(func_output_result);
        }

        let func_side_effect_result = match &test.expected_side_effect {
            Some(side_effect) => self.check_function_side_effects(pool, &side_effect).await?,
            None => TestResult::Passed
        };

        return Ok(func_side_effect_result);

    }

    async fn check_function_results(&self, pool: &PgPool, query:&str, expected_result: &Vec<HashMap<String,String>>) -> Result<TestResult> {
        todo!()
    }

    async fn check_function_side_effects(&self, pool: &PgPool, side_effect: &TestSideEffectConfig) -> Result<TestResult> {
        todo!()
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
                    Err(e) => bail!(e)
                }.to_string(),
            };
            result.insert(col.name().to_string(), value);
        }

        return Ok(result);
    }
}
