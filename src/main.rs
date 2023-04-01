use dir_creator::create_schema_dirs;
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use futures::TryStreamExt;
use dotenvy;
use anyhow;

mod dir_creator;


#[tokio::main]
async fn main() -> anyhow::Result<()> {

    dir_creator::init_directory_structure()?;
    dotenvy::from_filename("./.dbtvc/.env")?;

    let db_user = dotenvy::var("DB_USER")?;
    let db_pass = dotenvy::var("DB_PASSWORD")?;
    let db_port = dotenvy::var("DB_PORT")?;
    let db_name = dotenvy::var("DB_NAME")?;


    let connection_string = format!("postgres://{}:{}@{}/{}", db_user, db_pass, db_port,db_name);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&connection_string).await?;

    let mut rows  = sqlx::query("SELECT * FROM test_table")
        .fetch(& pool);

    while let Some(row) = rows.try_next().await? {
        let value:i32 = row.try_get("value")?;
        let text:String = row.try_get("some_text")?;

        println!("value: {:?}, text: {:?}", value,text);
    } 

    Ok(())
}
