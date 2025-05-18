use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

#[derive(Debug, Clone)]
pub enum IamResourceAddress {
    User(String),
    Role(String),
    Policy(String),
}

impl ResourceAddress for IamResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            IamResourceAddress::User(name) => PathBuf::from(format!("aws/iam/users/{}.ron", name)),
            IamResourceAddress::Role(name) => PathBuf::from(format!("aws/iam/roles/{}.ron", name)),
            IamResourceAddress::Policy(name) => {
                PathBuf::from(format!("aws/iam/policies/{}.ron", name))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Option<Self>, anyhow::Error> {
        // TODO Use try_collect to avoid the unwrap here! It's made for this!
        let path_components: Vec<&str> = path
            .components()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match &path_components[..] {
            ["aws", "iam", "users", name @ ..] if name.join("/").ends_with(".ron") => {
                let full_name = name.join("/");
                let full_name = full_name.strip_suffix(".ron").unwrap().to_string();
                Ok(Some(IamResourceAddress::User(full_name)))
            }
            ["aws", "iam", "roles", name @ ..] if name.join("/").ends_with(".ron") => {
                let full_name = name.join("/");
                let full_name = full_name.strip_suffix(".ron").unwrap().to_string();
                Ok(Some(IamResourceAddress::Role(full_name)))
            }
            ["aws", "iam", "policies", name @ ..] if name.join("/").ends_with(".ron") => {
                let full_name = name.join("/");
                let full_name = full_name.strip_suffix(".ron").unwrap().to_string();
                Ok(Some(IamResourceAddress::Policy(full_name)))
            }
            _ => Ok(None),
        }
    }
}
