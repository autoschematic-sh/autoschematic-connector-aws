use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum KmsResourceAddress {
    Key(String, String),         // (region, key_id)
    KeyPolicy(String, String),   // (region, key_id)
    Alias(String, String),       // (region, alias_name)
    KeyRotation(String, String), // (region, key_id)
}

impl ResourceAddress for KmsResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            KmsResourceAddress::Key(region, key_id) => PathBuf::from(format!("aws/kms/{region}/keys/{key_id}.ron")),
            KmsResourceAddress::KeyPolicy(region, key_id) => {
                PathBuf::from(format!("aws/kms/{region}/keys/{key_id}/policy.ron"))
            }
            KmsResourceAddress::Alias(region, alias_name) => {
                PathBuf::from(format!("aws/kms/{region}/aliases/{alias_name}.ron"))
            }
            KmsResourceAddress::KeyRotation(region, key_id) => {
                PathBuf::from(format!("aws/kms/{region}/keys/{key_id}/rotation.ron"))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "kms", region, "keys", key_id] if key_id.ends_with(".ron") => {
                let key_id = key_id.strip_suffix(".ron").unwrap().to_string();
                Ok(KmsResourceAddress::Key(region.to_string(), key_id))
            }
            ["aws", "kms", region, "keys", key_id, "policy.ron"] => {
                Ok(KmsResourceAddress::KeyPolicy(region.to_string(), key_id.to_string()))
            }
            ["aws", "kms", region, "aliases", alias_name] if alias_name.ends_with(".ron") => {
                let alias_name = alias_name.strip_suffix(".ron").unwrap().to_string();
                Ok(KmsResourceAddress::Alias(region.to_string(), alias_name))
            }
            ["aws", "kms", region, "keys", key_id, "rotation.ron"] => {
                Ok(KmsResourceAddress::KeyRotation(region.to_string(), key_id.to_string()))
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
