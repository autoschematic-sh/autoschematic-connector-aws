use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum SecretsManagerResourceAddress {
    Secret { region: String, name: String },
}

impl ResourceAddress for SecretsManagerResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            SecretsManagerResourceAddress::Secret { region, name } => {
                PathBuf::from(format!("aws/secretsmanager/{}/secrets/{}.ron", region, name))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "secretsmanager", region, "secrets", name @ ..] => {
                let name = name.join("/");
                if name.ends_with(".ron") {
                    let name = name.strip_suffix(".ron").unwrap().to_string();
                    Ok(SecretsManagerResourceAddress::Secret {
                        region: region.to_string(),
                        name,
                    })
                } else {
                    Err(invalid_addr_path(path))
                }
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
