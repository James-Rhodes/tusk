use anyhow::{Context, Result};
use dotenvy;
use sqlx::{postgres::PgPoolOptions, PgPool};

const MAX_DB_CONNECTIONS: u32 = 5;

struct SSHConnection {
    remote_ip_address: String,
    username: String,
    _local_port: String,
    _remote_port: String,
}

impl SSHConnection {
    fn new(
        local_ip_address: String,
        remote_ip_address: String,
        username: String,
        local_port: String,
        remote_port: String,
    ) -> Self {
        // Perform the SSH process call

        // Exit any existing port forwards on the port
        println!("Stopping any existing portforwards");
        std::process::Command::new("ssh")
            .arg("-q")
            .arg("-S")
            .arg("backup-socket")
            .arg("-O")
            .arg("exit")
            .arg(format!("{}@{}", username, remote_ip_address))
            .output()
            .expect(&format!(
                "Failed to close any ports currently on backup-socket to {}@{}",
                username, remote_ip_address
            ));

        // Forward the port
        println!("Forwarding the port");
        std::process::Command::new("ssh")
            .arg("-M")
            .arg("-S")
            .arg("backup-socket")
            .arg("-fNT")
            .arg("-L")
            .arg(format!(
                "{}:{}:{}",
                local_port, local_ip_address, remote_port
            ))
            .arg(format!("{}@{}", username, remote_ip_address))
            .output()
            .expect(&format!("Failed to forward port the local port {} to port {} of ip address {} for username {} \n Please try again with new ports or try again later", local_port, remote_port, remote_ip_address, username));

        return SSHConnection {
            remote_ip_address,
            username,
            _local_port: local_port,
            _remote_port: remote_port,
        };
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
            .arg(format!("{}@{}", self.username, self.remote_ip_address))
            .output()
            .expect(&format!(
                "Failed to close any ports currently on backup-socket to {}@{}",
                self.username, self.remote_ip_address
            ));
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
}

impl DbConnection {
    pub async fn new() -> Result<Self> {
        let (_env_vars, _ssh_connection) = Self::get_db_env_vars()?;

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

        return Ok(DbConnection {
            _env_vars,
            pool,
            connection_string,
            _ssh_connection,
        });
    }

    pub fn get_connection_string(&self) -> &str {
        return &self.connection_string;
    }

    pub fn get_connection_pool(&self) -> &PgPool {
        return &self.pool;
    }

    fn get_db_env_vars() -> Result<(DbEnvVars, Option<SSHConnection>)> {
        // TODO: Add context to all of the below errors so that they make more sense for users
        dotenvy::from_filename("./.tusk/.env")?;

        let db_user = dotenvy::var("DB_USER").context("Required environment variable DB_USER is not set in ./.tusk/.env please set this to continue")?;
        let db_pass = dotenvy::var("DB_PASSWORD").context("Required environment variable DB_PASSWORD is not set in ./.tusk/.env please set this to continue")?;
        let db_host = dotenvy::var("DB_HOST").context("Required environment variable DB_HOST is not set in ./.tusk/.env please set this to continue")?;
        let mut db_port = dotenvy::var("DB_PORT").context("Required environment variable DB_PORT is not set in ./.tusk/.env please set this to continue")?;
        let db_name = dotenvy::var("DB_NAME").context("Required environment variable DB_NAME is not set in ./.tusk/.env please set this to continue")?;

        let use_ssh = dotenvy::var("USE_SSH");
        let ssh_remote_ip_address = dotenvy::var("SSH_REMOTE_IP_ADDRESS");
        let ssh_user = dotenvy::var("SSH_USERNAME");
        let ssh_local_port = dotenvy::var("SSH_LOCAL_PORT");
        let ssh_remote_port = dotenvy::var("SSH_REMOTE_PORT");

        let ssh_connection: Option<SSHConnection> = if let Ok(use_ssh) = use_ssh {
            if use_ssh == "TRUE" {
                db_port = ssh_local_port.context("Required environment variable SSH_LOCAL_PORT is not set in ./.tusk/.env please set this to continue")?;
                Some(SSHConnection::new(
                    db_host.clone(),
                    ssh_remote_ip_address.context("Required environment variable SSH_REMOTE_IP_ADDRESS is not set in ./.tusk/.env please set this to continue")?,
                    ssh_user.context("Required environment variable SSH_USERNAME is not set in ./.tusk/.env please set this to continue")?,
                    db_port.clone(),
                    ssh_remote_port.context("Required environment variable SSH_REMOTE_PORT is not set in ./.tusk/.env please set this to continue")?,
                ))
            } else {
                None
            }
        } else {
            None
        };

        return Ok((
            DbEnvVars {
                db_user,
                db_pass,
                db_host,
                db_port,
                db_name,
            },
            ssh_connection,
        ));
    }
}
