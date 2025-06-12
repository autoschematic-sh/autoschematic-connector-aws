use anyhow::bail;
use autoschematic_core::connector::{Resource, ResourceAddress};
use ron::ser::PrettyConfig;

use super::addr::CloudFrontResourceAddress;

// Define the CloudFrontResource enum
pub enum CloudFrontResource {}

// Implement the Resource trait
impl Resource for CloudFrontResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        Ok(Vec::new())
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr_option = CloudFrontResourceAddress::from_path(&addr.to_path_buf())?;

        bail!("Invalid CloudFront resource address")
    }
}
