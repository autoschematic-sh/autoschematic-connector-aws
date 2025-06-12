use autoschematic_core::connector::{Resource, ResourceAddress};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::RON;

use super::{addr::EcrResourceAddress, tags::Tags};

// Define encryption configuration struct
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct EncryptionConfiguration {
    pub encryption_type: String, // AES256 or KMS
    pub kms_key: Option<String>, // ARN of the KMS key when encryption_type is KMS
}

// Define image scanning configuration struct
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ImageScanningConfiguration {
    pub scan_on_push: bool,
}

// Define resource structs
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Repository {
    pub encryption_configuration: Option<EncryptionConfiguration>,
    pub image_tag_mutability: Option<String>, // MUTABLE or IMMUTABLE
    pub image_scanning_configuration: Option<ImageScanningConfiguration>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RepositoryPolicy {
    pub policy_document: ron::Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct LifecyclePolicy {
    pub lifecycle_policy_text: ron::Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RegistryPolicy {
    pub policy_document: ron::Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PullThroughCacheRule {
    pub upstream_registry_url: String,
    pub credential_arn: Option<String>,
}

// Define the EcrResource enum
pub enum EcrResource {
    Repository(Repository),
    RepositoryPolicy(RepositoryPolicy),
    LifecyclePolicy(LifecyclePolicy),
    RegistryPolicy(RegistryPolicy),
    PullThroughCacheRule(PullThroughCacheRule),
}

// Implement the Resource trait
impl Resource for EcrResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default();
        match self {
            EcrResource::Repository(repo) => Ok(RON.to_string_pretty(&repo, pretty_config)?.into()),
            EcrResource::RepositoryPolicy(policy) => Ok(RON.to_string_pretty(&policy, pretty_config)?.into()),
            EcrResource::LifecyclePolicy(policy) => Ok(RON.to_string_pretty(&policy, pretty_config)?.into()),
            EcrResource::RegistryPolicy(policy) => Ok(RON.to_string_pretty(&policy, pretty_config)?.into()),
            EcrResource::PullThroughCacheRule(rule) => Ok(RON.to_string_pretty(&rule, pretty_config)?.into()),
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = EcrResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;

        match addr {
            EcrResourceAddress::Repository { region, name } => Ok(EcrResource::Repository(RON.from_str(s)?)),
            EcrResourceAddress::RepositoryPolicy { region, name } => Ok(EcrResource::RepositoryPolicy(RON.from_str(s)?)),
            EcrResourceAddress::LifecyclePolicy { region, name } => Ok(EcrResource::LifecyclePolicy(RON.from_str(s)?)),
            EcrResourceAddress::RegistryPolicy { region } => Ok(EcrResource::RegistryPolicy(RON.from_str(s)?)),
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => {
                Ok(EcrResource::PullThroughCacheRule(RON.from_str(s)?))
            }
        }
    }
}
