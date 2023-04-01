pub mod init;

pub trait Action {
    fn execute(&self) -> anyhow::Result<()>;
}
