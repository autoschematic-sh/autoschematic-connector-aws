use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::CloudFrontConnector;

mod connector;
// pub mod client_cache;
mod addr;
mod config;
mod op;
// pub mod op_impl;
mod resource;
mod tags;
mod util;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<CloudFrontConnector>().await?;
    Ok(())
}
