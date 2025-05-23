use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
};

use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::SecretsManagerResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Secret {
    pub description: Option<String>,
    pub kms_key_id: Option<String>,
    pub secret_ref: Option<String>,
    pub policy_document: ron::Value,
    pub tags: Tags,
}

pub enum SecretsManagerResource {
    Secret(Secret),
}

impl Resource for SecretsManagerResource {
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default();
        match self {
            SecretsManagerResource::Secret(secret) => Ok(RON.to_string_pretty(secret, pretty_config)?.into()),
        }
    }

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = SecretsManagerResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s.as_bytes())?;

        match addr {
            SecretsManagerResourceAddress::Secret { region, name } => {
                let secret: Secret = RON.from_str(s)?;
                Ok(SecretsManagerResource::Secret(secret))
            }
        }
    }
}
