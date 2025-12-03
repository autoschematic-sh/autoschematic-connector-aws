use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::IamConnector;

pub mod connector;
pub mod addr;
pub mod op;
pub mod resource;
pub mod tags;
pub mod task;
pub mod util;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<IamConnector>().await?;
    Ok(())
}