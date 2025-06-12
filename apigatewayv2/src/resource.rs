use anyhow::bail;
use autoschematic_core::connector::{Resource, ResourceAddress};
use ron::ser::PrettyConfig;

use super::addr::ApiGatewayV2ResourceAddress;

// Define the ApiGatewayV2Resource enum
pub enum ApiGatewayV2Resource {}

// Implement the Resource trait
impl Resource for ApiGatewayV2Resource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        Ok(Vec::new())
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = ApiGatewayV2ResourceAddress::from_path(&addr.to_path_buf())?;

        bail!("Invalid ApiGatewayV2 resource address")
    }
}
