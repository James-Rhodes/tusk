pub mod error_handling;

use anyhow::{Context, Result, bail};
use dotenvy;
use sqlx::{postgres::PgPoolOptions, PgPool};

const MAX_DB_CONNECTIONS: u32 = 5;

struct SSHConnection {
    ssh_host: String,
    user: String,
    _local_bind_port: String,
    _db_port: String,
}

impl SSHConnection {
    fn new(
        db_host: String,
        ssh_host: String,
        user: String,
        local_bind_port: String,
        db_port: String,
    ) -> Result<Self> {
        // Perform the SSH process call

        // Exit any existing port forwards on the port
        println!("Stopping any existing portforwards");
        std::process::Command::new("ssh")
            .arg("-q")
            .arg("-S")
            .arg("backup-socket")
            .arg("-O")
            .arg("exit")
            .arg(format!("{}@{}", user, ssh_host))
            .output()
            .unwrap_or_else(|_| panic!("Failed to close any ports currently on backup-socket to {}@{}",
                user, ssh_host));

        // Forward the port
        println!("Forwarding the port");
        let command = std::process::Command::new("ssh")
            .arg("-M")
            .arg("-S")
            .arg("backup-socket")
            .arg("-fNT")
            .arg("-L")
            .arg(format!(
                "{}:{}:{}",
                local_bind_port, db_host, db_port
            ))
            .arg(format!("{}@{}", user, ssh_host))
            .output()
            .context(format!("Failed to forward port the local port {} to port {} of ip address {} for username {} \n Please try again with new ports or try again later", local_bind_port, db_port, ssh_host, user))?;
        let command_err = std::str::from_utf8(&command.stderr[..]).unwrap_or("").to_owned();
        if !command_err.is_empty() {
            bail!(command_err)
        }
 

        Ok(SSHConnection {
            ssh_host,
            user,
            _local_bind_port: local_bind_port,
            _db_port: db_port,
        })
    }
}

impl Drop for SSHConnection {
    fn drop(&mut self) {
        // Close the port forward
        std::process::Command::new("ssh")
            .arg("-q")
            .arg("-S")
            .arg("backup-socket")
            .arg("-O")
            .arg("exit")
            .arg(format!("{}@{}", self.user, self.ssh_host))
            .output()
            .unwrap_or_else(|_| panic!("Failed to close any ports currently on backup-socket to {}@{}",
                self.user, self.ssh_host));
    }
}

struct DbEnvVars {
    db_user: String,
    db_pass: String,
    db_host: String,
    db_port: String,
    db_name: String,
}

pub struct DbConnection {
    _env_vars: DbEnvVars,
    pool: PgPool,
    connection_string: String,
    _ssh_connection: Option<SSHConnection>,
    pg_bin_path: String
}

impl DbConnection {
    pub async fn new() -> Result<Self> {
        let (_env_vars, _ssh_connection) = Self::get_db_env_vars()?;

        let pg_bin_path = dotenvy::var("PG_BIN_PATH").unwrap_or_else(|_| String::from("pg_dump"));
        let connection_string = format!(
            "postgres://{}:{}@{}:{}/{}",
            _env_vars.db_user,
            _env_vars.db_pass,
            _env_vars.db_host,
            _env_vars.db_port,
            _env_vars.db_name
        );

        let pool = PgPoolOptions::new()
            .max_connections(MAX_DB_CONNECTIONS)
            .connect(&connection_string)
            .await?;

        Ok(DbConnection {
            _env_vars,
            pool,
            connection_string,
            _ssh_connection,
            pg_bin_path,
        })
    }

    pub fn get_connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn get_pg_bin_path(&self) -> &str {
        &self.pg_bin_path
    }

    pub fn get_connection_pool(&self) -> &PgPool {
        &self.pool
    }

    fn get_db_env_vars() -> Result<(DbEnvVars, Option<SSHConnection>)> {
        // TODO: Add context to all of the below errors so that they make more sense for users
        dotenvy::from_filename("./.tusk/.env")?;

        let db_user = dotenvy::var("DB_USER").context("Required environment variable DB_USER is not set in ./.tusk/.env please set this to continue")?;
        let db_pass = dotenvy::var("DB_PASSWORD").context("Required environment variable DB_PASSWORD is not set in ./.tusk/.env please set this to continue")?;
        let mut db_host = dotenvy::var("DB_HOST").context("Required environment variable DB_HOST is not set in ./.tusk/.env please set this to continue")?;
        let mut db_port = dotenvy::var("DB_PORT").context("Required environment variable DB_PORT is not set in ./.tusk/.env please set this to continue")?;
        let db_name = dotenvy::var("DB_NAME").context("Required environment variable DB_NAME is not set in ./.tusk/.env please set this to continue")?;

        let use_ssh = dotenvy::var("USE_SSH");
        let ssh_host = dotenvy::var("SSH_HOST");
        let ssh_user = dotenvy::var("SSH_USER");
        let ssh_local_bind_port = dotenvy::var("SSH_LOCAL_BIND_PORT");

        let ssh_connection: Option<SSHConnection> = if let Ok(use_ssh) = use_ssh {
            if use_ssh == "TRUE" {
                let remote_db_port = db_port.clone();
                let remote_db_host = db_host.clone();
                db_host = String::from("127.0.0.1"); // For pg connection we are now connecting through local host
                db_port = ssh_local_bind_port.context("Required environment variable SSH_LOCAL_BIND_PORT is not set in ./.tusk/.env please set this to continue")?;
                Some(SSHConnection::new(
                    remote_db_host,
                    ssh_host.context("Required environment variable SSH_HOST is not set in ./.tusk/.env please set this to continue")?,
                    ssh_user.context("Required environment variable SSH_USER is not set in ./.tusk/.env please set this to continue")?,
                    db_port.clone(),
                    remote_db_port
                )?)
            } else {
                None
            }
        } else {
            None
        };

        Ok((
            DbEnvVars {
                db_user,
                db_pass,
                db_host,
                db_port,
                db_name,
            },
            ssh_connection,
        ))
    }
}
