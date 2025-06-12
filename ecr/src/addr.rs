use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum EcrResourceAddress {
    Repository { region: String, name: String },
    RepositoryPolicy { region: String, name: String },
    LifecyclePolicy { region: String, name: String },
    RegistryPolicy { region: String },
    PullThroughCacheRule { region: String, prefix: String },
}

impl ResourceAddress for EcrResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            EcrResourceAddress::Repository { region, name } => {
                PathBuf::from(format!("aws/ecr/{}/repositories/{}.ron", region, name))
            }
            EcrResourceAddress::RepositoryPolicy { region, name } => {
                PathBuf::from(format!("aws/ecr/{}/repositories/{}/policy.ron", region, name))
            }
            EcrResourceAddress::LifecyclePolicy { region, name } => {
                PathBuf::from(format!("aws/ecr/{}/repositories/{}/lifecycle_policy.ron", region, name))
            }
            EcrResourceAddress::RegistryPolicy { region } => PathBuf::from(format!("aws/ecr/{}/registry_policy.ron", region)),
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => {
                PathBuf::from(format!("aws/ecr/{}/pull_through_cache_rules/{}.ron", region, prefix))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "ecr", region, "repositories", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(EcrResourceAddress::Repository {
                    region: region.to_string(),
                    name,
                })
            }
            ["aws", "ecr", region, "repositories", name, "policy.ron"] => Ok(EcrResourceAddress::RepositoryPolicy {
                region: region.to_string(),
                name:   name.to_string(),
            }),
            ["aws", "ecr", region, "repositories", repo_name, "lifecycle_policy.ron"] => {
                Ok(EcrResourceAddress::LifecyclePolicy {
                    region: region.to_string(),
                    name:   repo_name.to_string(),
                })
            }
            ["aws", "ecr", region, "registry_policy.ron"] => Ok(EcrResourceAddress::RegistryPolicy {
                region: region.to_string(),
            }),
            ["aws", "ecr", region, "pull_through_cache_rules", prefix] if prefix.ends_with(".ron") => {
                let prefix = prefix.strip_suffix(".ron").unwrap().to_string();
                Ok(EcrResourceAddress::PullThroughCacheRule {
                    region: region.to_string(),
                    prefix,
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
