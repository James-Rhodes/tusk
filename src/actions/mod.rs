use async_trait::async_trait;

pub mod init;
pub mod refresh_inventory;
pub mod sync;
pub mod push;

#[async_trait]
pub trait Action {
    async fn execute(&self) -> anyhow::Result<()>;
}
