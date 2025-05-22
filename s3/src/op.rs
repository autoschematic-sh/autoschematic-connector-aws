use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};


use super::{resource::{Acl, PublicAccessBlock, S3Bucket}, tags::Tags};


#[derive(Debug, Serialize, Deserialize)]
pub enum S3ConnectorOp {
    CreateBucket(S3Bucket),
    UpdateBucketPolicy(Option<ron::Value>, Option<ron::Value>),
    UpdateBucketPublicAccessBlock(Option<PublicAccessBlock>),
    UpdateBucketAcl(Acl, Acl),
    UpdateBucketTags(Tags, Tags),
    DeleteBucket,
}

impl ConnectorOp for S3ConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}