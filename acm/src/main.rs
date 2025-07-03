use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::AcmConnector;

pub mod connector;
pub mod addr;
pub mod op;
pub mod resource;
pub mod config;
pub mod tags;
pub mod util;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<AcmConnector>().await?;
    Ok(())
}
