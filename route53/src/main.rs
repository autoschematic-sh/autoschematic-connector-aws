use autoschematic_core::tarpc_bridge::tarpc_connector_main;
use connector::Route53Connector;

pub mod connector;
// pub mod client_cache;
// pub mod config;
pub mod addr;
pub mod op;
// pub mod op_impl;
pub mod resource;
// pub mod tags;
pub mod util;

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tarpc_connector_main::<Route53Connector>().await?;
    Ok(())
}
