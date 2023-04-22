use anyhow::Result;
use dotenvy;
use sqlx::{postgres::PgPoolOptions, PgPool};

const MAX_DB_CONNECTIONS: u32 = 5;

struct DbEnvVars {
    db_user: String,
    db_pass: String,
    db_port: String,
    db_name: String,
}

pub async fn get_db_connection() -> Result<PgPool> {
    let DbEnvVars {
        db_user,
        db_pass,
        db_port,
        db_name,
    } = get_db_env_vars()?;

    let connection_string = format!("postgres://{}:{}@{}/{}", db_user, db_pass, db_port, db_name);

    let pool = PgPoolOptions::new()
        .max_connections(MAX_DB_CONNECTIONS)
        .connect(&connection_string)
        .await?;

    return Ok(pool);
}

fn get_db_env_vars() -> Result<DbEnvVars> {
    dotenvy::from_filename("./.tusk/.env")?;

    let db_user = dotenvy::var("DB_USER")?;
    let db_pass = dotenvy::var("DB_PASSWORD")?;
    let db_port = dotenvy::var("DB_PORT")?;
    let db_name = dotenvy::var("DB_NAME")?;

    return Ok(DbEnvVars {
        db_user,
        db_pass,
        db_port,
        db_name,
    });
}
