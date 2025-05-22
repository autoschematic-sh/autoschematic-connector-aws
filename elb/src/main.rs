use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::ElbConnector;

pub mod connector;
pub mod util;
pub mod addr;
pub mod config;
pub mod op;
pub mod resource;
pub mod tags;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<ElbConnector>().await?;
    Ok(())
}
