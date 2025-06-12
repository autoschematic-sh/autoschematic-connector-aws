use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::EcrConnector;

pub mod connector;
// pub mod client_cache;
pub mod addr;
pub mod config;
pub mod op;
pub mod op_impl;
pub mod resource;
pub mod tags;
pub mod util;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<EcrConnector>().await?;
    Ok(())
}
