use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

use std::collections::HashSet;

use autoschematic_core::connector::Resource;
use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::{PrettyConfig, RON};

use super::addr::IamResourceAddress;
use super::tags::Tags;

#[derive(Debug, Clone)]
pub enum IamTaskAddress {
    RotateCredential { name: String },
}

impl ResourceAddress for IamTaskAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            IamTaskAddress::RotateCredential { name } => PathBuf::from(format!("aws/iam/tasks/rotate-credential/{name}.ron")),
        }
    }

    fn from_path(path: &Path) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "iam", "tasks", "rotate-credential", name] if name.ends_with(".ron") => {
                Ok(IamTaskAddress::RotateCredential { name: name.to_string() })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub r#type: Option<String>,
    pub principal: String,
    pub output_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RotateCredential {
    pub credentials: Vec<Credential>,
}

pub enum IamTask {
    RotateCredential(RotateCredential),
}

impl Resource for IamTask {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            IamTask::RotateCredential(rotate_credential) => match RON.to_string_pretty(&rotate_credential, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = IamTaskAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        // IamResourceAddress::User { path, name } => Ok(IamResource::User(RON.from_str(s)?)),
        match addr {
            IamTaskAddress::RotateCredential { .. } => Ok(IamTask::RotateCredential(RON.from_str(s)?)),
        }
    }
}
