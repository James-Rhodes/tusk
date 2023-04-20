pub mod function_syncer;
pub mod table_ddl_syncer;

use std::pin::Pin;

use crate::actions::sync::DDL;
use anyhow::Result;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait Syncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>>;

    fn get<'conn>(pool: &'conn PgPool, schema: &'conn str, items: &'conn Vec<String>) -> Result<RowStream<'conn>>;
}
