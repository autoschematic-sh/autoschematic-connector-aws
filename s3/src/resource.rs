use std::{
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStrExt,
};

use autoschematic_core::{
    connector::{ConnectorOp, Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::S3ResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Grant {
    pub grantee_id: String,
    pub permission: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Acl {
    pub owner_id: String,
    pub grants: Vec<Grant>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PublicAccessBlock {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct S3Bucket {
    pub policy: Option<ron::Value>,
    pub public_access_block: Option<PublicAccessBlock>,
    pub acl: Acl,
    pub tags: Tags,
}

pub enum S3Resource {
    Bucket(S3Bucket),
    // Object(S3Object),
}

impl Resource for S3Resource {
    fn to_os_string(&self) -> Result<OsString, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);
        // .extensions(ron::extensions::Extensions::IMPLICIT_SOME);
        match self {
            S3Resource::Bucket(bucket) => Ok(RON.to_string_pretty(&bucket, pretty_config)?.into()),
        }
    }

    fn from_os_str(addr: &impl ResourceAddress, s: &OsStr) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = S3ResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s.as_bytes())?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => Ok(S3Resource::Bucket(RON.from_str(s)?)),
        }
    }
}
