pub mod function_syncer;
pub mod table_ddl_syncer;
pub mod table_data_syncer;

use std::pin::Pin;

use crate::actions::sync::DDL;
use anyhow::Result;
use futures::Stream;
use sqlx::PgPool;

pub type RowStream<'conn> = Pin<Box<dyn Stream<Item = Result<DDL, sqlx::Error>> + Send + 'conn>>;

pub trait SQLSyncer {
    // This returns all the DDL from a postgres query as a stream for writing manually to a file
    fn get_all<'conn>(pool: &'conn PgPool, schema: &'conn str) -> Result<RowStream<'conn>>;

    // This returns the DDL from a Postgres query as a stream for writing manually to a file
    fn get<'conn>(pool: &'conn PgPool, schema: &'conn str, items: &'conn Vec<String>) -> Result<RowStream<'conn>>;
}

pub trait PgDumpSyncer {
    // This one will write all of the DDL to files
    fn get_all(schema: &str) -> Result<()>;

    // This one will write the ones in items to DDL files
    fn get(schema: &str, items: &Vec<String>) -> Result<()>;
}
