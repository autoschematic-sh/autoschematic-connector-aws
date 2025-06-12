use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum IamResourceAddress {
    User { path: String, name: String },
    Role { path: String, name: String },
    Group { path: String, name: String },
    Policy { path: String, name: String },
}

impl ResourceAddress for IamResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            IamResourceAddress::User { path, name } => PathBuf::from(format!("aws/iam/users{}{}.ron", path, name)),
            IamResourceAddress::Role { path, name } => PathBuf::from(format!("aws/iam/roles{}{}.ron", path, name)),
            IamResourceAddress::Group { path, name } => PathBuf::from(format!("aws/iam/groups{}{}.ron", path, name)),
            IamResourceAddress::Policy { path, name } => PathBuf::from(format!("aws/iam/policies{}{}.ron", path, name)),
        }
    }
    // IamResourceAddress::User{=>
    // IamResourceAddress::Role(name) => PathBuf::from(format!("aws/iam/roles/{}.ron", name)),
    // IamResourceAddress::Group(name) => PathBuf::from(format!("aws/iam/group/{}.ron", name)),
    // IamResourceAddress::Policy(name) => PathBuf::from(format!("aws/iam/policies/{}.ron", name)),

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        // TODO Use try_collect to avoid the unwrap here! It's made for this!
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "iam", "users", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = String::from("/");
                Ok(IamResourceAddress::User { path, name })
            }
            ["aws", "iam", "users", path @ .., name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = path.join("/");
                let path = format!("/{}/", path);
                Ok(IamResourceAddress::User { path, name })
            }
            ["aws", "iam", "groups", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = String::from("/");
                Ok(IamResourceAddress::Group { path, name })
            }
            ["aws", "iam", "groups", path @ .., name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = path.join("/");
                let path = format!("/{}/", path);
                Ok(IamResourceAddress::Group { path, name })
            }
            ["aws", "iam", "roles", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = String::from("/");
                Ok(IamResourceAddress::Role { path, name })
            }
            ["aws", "iam", "roles", path @ .., name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = path.join("/");
                let path = format!("/{}/", path);
                Ok(IamResourceAddress::Role { path, name })
            }
            ["aws", "iam", "policies", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = String::from("/");
                Ok(IamResourceAddress::Policy { path, name })
            }
            ["aws", "iam", "policies", path @ .., name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                let path = path.join("/");
                let path = format!("/{}/", path);
                Ok(IamResourceAddress::Policy { path, name })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
