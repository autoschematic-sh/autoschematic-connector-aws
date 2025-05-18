use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

use autoschematic_core::connector::{ConnectorOp, Resource, ResourceAddress};
use serde::{Deserialize, Serialize};

use autoschematic_core::util::{PrettyConfig, RON};

use super::addr::IamResourceAddress;
use super::tags::Tags;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IamUser {
    pub attached_policies: Vec<String>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IamRole {
    pub attached_policies: Vec<String>,
    pub assume_role_policy_document: Option<ron::Value>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IamPolicy {
    pub policy_document: ron::Value,
    pub tags: Tags,
}

pub enum IamResource {
    User(IamUser),
    Role(IamRole),
    Policy(IamPolicy),
}

impl Resource for IamResource {
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            IamResource::User(user) => match RON.to_string_pretty(&user, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            IamResource::Role(role) => match RON.to_string_pretty(&role, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            IamResource::Policy(policy) => match RON.to_string_pretty(&policy, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Option<Self>, anyhow::Error>
    where
        Self: Sized,
    {
        let Some(addr) = IamResourceAddress::from_path(&addr.to_path_buf())? else {
            return Ok(None);
        };
        let s = str::from_utf8(s.as_bytes())?;
        match addr {
            IamResourceAddress::User(_name) => {
                return Ok(Some(IamResource::User(RON.from_str(s)?)));
            }
            IamResourceAddress::Role(_name) => {
                return Ok(Some(IamResource::Role(RON.from_str(s)?)));
            }
            IamResourceAddress::Policy(_name) => {
                return Ok(Some(IamResource::Policy(RON.from_str(s)?)));
            }
        }
    }
}
