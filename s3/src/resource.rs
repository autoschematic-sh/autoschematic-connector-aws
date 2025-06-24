use autoschematic_core::{
    connector::{ConnectorOp, Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::S3ResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub grantee_id: String,
    pub permission: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Acl {
    pub owner_id: String,
    pub grants:   Vec<Grant>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PublicAccessBlock {
    pub block_public_acls: bool,
    pub ignore_public_acls: bool,
    pub block_public_policy: bool,
    pub restrict_public_buckets: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct S3Bucket {
    pub policy: Option<ron::Value>,
    pub public_access_block: Option<PublicAccessBlock>,
    pub acl: Option<Acl>,
    pub tags: Tags,
}

pub enum S3Resource {
    Bucket(S3Bucket),
}

impl Resource for S3Resource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);
        // .extensions(ron::extensions::Extensions::IMPLICIT_SOME);
        match self {
            S3Resource::Bucket(bucket) => Ok(RON.to_string_pretty(&bucket, pretty_config)?.into()),
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = S3ResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => Ok(S3Resource::Bucket(RON.from_str(s)?)),
        }
    }
}
