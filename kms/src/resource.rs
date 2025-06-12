use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::KmsResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KmsKey {
    pub description: String,
    pub key_usage: String,
    pub customer_master_key_spec: String,
    pub origin: String,
    pub multi_region: bool,
    pub enabled: bool,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KmsKeyPolicy {
    pub policy_document: ron::Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KmsAlias {
    pub target_key_id: String,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct KmsKeyRotation {
    pub enabled: bool,
}

pub enum KmsResource {
    Key(KmsKey),
    KeyPolicy(KmsKeyPolicy),
    Alias(KmsAlias),
    KeyRotation(KmsKeyRotation),
}

impl Resource for KmsResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default();

        match self {
            KmsResource::Key(key) => match RON.to_string_pretty(&key, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            KmsResource::KeyPolicy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            KmsResource::Alias(alias) => match RON.to_string_pretty(&alias, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            KmsResource::KeyRotation(rotation) => match RON.to_string_pretty(&rotation, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = KmsResourceAddress::from_path(&addr.to_path_buf())?;
        let s = str::from_utf8(s)?;
        match addr {
            KmsResourceAddress::Key(_region, _key_id) => Ok(KmsResource::Key(RON.from_str(s)?)),
            KmsResourceAddress::KeyPolicy(_region, _key_id) => Ok(KmsResource::KeyPolicy(RON.from_str(s)?)),
            KmsResourceAddress::Alias(_region, _alias_name) => Ok(KmsResource::Alias(RON.from_str(s)?)),
            KmsResourceAddress::KeyRotation(_region, _key_id) => Ok(KmsResource::KeyRotation(RON.from_str(s)?)),
        }
    }
}
