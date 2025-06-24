use std::path::Path;

use anyhow::bail;
use autoschematic_core::{
    connector::{GetResourceOutput, Resource, ResourceAddress},
    util::RON,
};

use crate::{addr::S3ResourceAddress, resource, tags::Tags};

use super::S3Connector;

impl S3Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                let Ok(list_results) = client.list_buckets().bucket_region(region).prefix(&name).send().await else {
                    return Ok(None);
                };

                let Some(bucket) = list_results.buckets.and_then(|b| b.first().cloned()) else {
                    return Ok(None);
                };

                if bucket.name != Some(name.clone()) {
                    return Ok(None);
                }

                let tagging_output = client.get_bucket_tagging().bucket(&name).send().await;

                let policy_output = client.get_bucket_policy().bucket(&name).send().await;

                let public_access_block_output = client.get_public_access_block().bucket(&name).send().await;

                let policy = if let Ok(policy_output) = policy_output {
                    match policy_output.policy {
                        Some(policy_string) => {
                            let json_s = urlencoding::decode(&policy_string)?;
                            let val: serde_json::Value = serde_json::from_str(&json_s)?;

                            let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;
                            Some(rval)
                        }
                        None => None,
                    }
                } else {
                    None
                };

                let public_access_block = if let Ok(public_access_block_output) = public_access_block_output {
                    public_access_block_output
                        .public_access_block_configuration
                        .map(|conf| resource::PublicAccessBlock {
                            block_public_acls: conf.block_public_acls.unwrap_or(false),
                            ignore_public_acls: conf.ignore_public_acls.unwrap_or(false),
                            block_public_policy: conf.block_public_policy.unwrap_or(false),
                            restrict_public_buckets: conf.restrict_public_buckets.unwrap_or(false),
                        })
                } else {
                    None
                };

                let acl = if let Ok(acl_output) = client.get_bucket_acl().bucket(&name).send().await {
                    let Some(owner) = acl_output.owner else {
                        bail!("ACL Output has no owner")
                    };
                    let mut grants: Vec<resource::Grant> = Vec::new();
                    for grant in acl_output.grants.unwrap_or_default() {
                        if let Some(grantee) = grant.grantee {
                            if let Some(permission) = grant.permission {
                                grants.push(resource::Grant {
                                    grantee_id: grantee.id.unwrap_or_default(),
                                    permission: permission.as_str().to_string(),
                                })
                            }
                        }
                    }

                    Some(resource::Acl {
                        owner_id: owner.id.unwrap_or_default(),
                        grants,
                    })
                } else {
                    None
                };

                let tags = if let Ok(tagging_output) = tagging_output {
                    tagging_output.tag_set.into()
                } else {
                    Tags::default()
                };

                let bucket = resource::S3Bucket {
                    policy,
                    public_access_block,
                    acl: acl,
                    tags,
                };

                Ok(Some(GetResourceOutput {
                    resource_definition: resource::S3Resource::Bucket(bucket).to_bytes()?,
                    outputs: None,
                }))
            }
        }
    }
}
