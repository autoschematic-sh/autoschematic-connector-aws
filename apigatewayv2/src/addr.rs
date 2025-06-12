use std::path::{Path, PathBuf};

use autoschematic_core::{
    connector::ResourceAddress,
    error_util::invalid_addr_path,
};

type Region = String;
type SecretName = String;

#[derive(Debug, Clone)]
pub enum ApiGatewayV2ResourceAddress {}

impl ResourceAddress for ApiGatewayV2ResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        PathBuf::new()
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path
            .components()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        Err(invalid_addr_path(path))
    }
}
