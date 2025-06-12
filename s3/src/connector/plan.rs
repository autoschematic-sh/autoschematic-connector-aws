use std::path::Path;

use autoschematic_core::{
    connector::{OpPlanOutput, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::S3ResourceAddress, op::S3ConnectorOp, resource};

use super::S3Connector;

impl S3Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_bucket)) => {
                    let new_bucket: resource::S3Bucket = RON.from_str(&new_bucket)?;
                    Ok(vec![connector_op!(
                        S3ConnectorOp::CreateBucket(new_bucket),
                        format!("Create new bucket {} in region {}", name, region)
                    )])
                }

                (Some(_old_bucket), None) => Ok(vec![connector_op!(
                    S3ConnectorOp::DeleteBucket,
                    format!("DELETE bucket {} in region {}", name, region)
                )]),
                (Some(old_bucket), Some(new_bucket)) => {
                    let old_bucket: resource::S3Bucket = RON.from_str(&old_bucket).unwrap();
                    let new_bucket: resource::S3Bucket = RON.from_str(&new_bucket).unwrap();
                    let mut ops = Vec::new();

                    if old_bucket.policy != new_bucket.policy {
                        let diff = diff_ron_values(&old_bucket.policy, &new_bucket.policy).unwrap_or_default();
                        ops.push(connector_op!(
                            S3ConnectorOp::UpdateBucketPolicy(old_bucket.policy, new_bucket.policy,),
                            format!("Modify Policy for S3 bucket `{}`\n{}", name, diff)
                        ));
                    }

                    if old_bucket.acl != new_bucket.acl {
                        let diff = diff_ron_values(&old_bucket.acl, &new_bucket.acl).unwrap_or_default();
                        ops.push(connector_op!(
                            S3ConnectorOp::UpdateBucketAcl(old_bucket.acl, new_bucket.acl,),
                            format!("Modify ACL for S3 bucket `{}`\n{}", name, diff)
                        ));
                    }

                    if old_bucket.tags != new_bucket.tags {
                        let diff = diff_ron_values(&old_bucket.tags, &new_bucket.tags).unwrap_or_default();
                        ops.push(connector_op!(
                            S3ConnectorOp::UpdateBucketTags(old_bucket.tags, new_bucket.tags,),
                            format!("Modify tags for S3 bucket `{}`\n{}", name, diff)
                        ));
                    }

                    Ok(ops)
                }
            },
        }
    }
}
