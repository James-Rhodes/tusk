pub mod init;
pub mod refresh_inventory;

pub trait Action {
    fn execute(&self) -> anyhow::Result<()>;
}
