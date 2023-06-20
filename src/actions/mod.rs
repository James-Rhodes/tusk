use async_trait::async_trait;

pub mod init;
pub mod fetch;
pub mod pull;
pub mod push;

#[async_trait]
pub trait Action {
    async fn execute(&self) -> anyhow::Result<()>;
}
