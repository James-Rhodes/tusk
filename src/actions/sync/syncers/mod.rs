pub mod function_syncer;

use std::pin::Pin;

use crate::actions::sync::DDL;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait Syncer {
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> RowStream<'conn>;

    fn get<'conn>(pool: &'conn PgPool, schema: &str, items: Vec<String>) -> RowStream<'conn>;
}
