use std::path::Path;

use anyhow::Context;
use autoschematic_core::connector::{ConnectorOp, OpExecResponse, ResourceAddress};
use aws_sdk_s3::types::CreateBucketConfiguration;

use crate::{addr::S3ResourceAddress, op::S3ConnectorOp};

use super::S3Connector;

impl S3Connector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;
        let op = S3ConnectorOp::from_str(op)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => {
                match op {
                    S3ConnectorOp::CreateBucket(bucket) => {
                        let client = self.get_or_init_client(&region).await?;

                        let create_bucket_configuration = if region == "us-east-1" {
                            None
                        } else {
                            Some(CreateBucketConfiguration::builder()
                                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::try_parse(&region)?)
                                .build())
                        };

                        // Create the bucket
                        match client
                            .create_bucket()
                            .set_create_bucket_configuration(create_bucket_configuration)
                            .bucket(&name)
                            .send()
                            .await
                        {
                            Ok(_) => {
                                if let Some(policy) = &bucket.policy {
                                    let policy_json =
                                        serde_json::to_string(&policy).context("Failed to serialize bucket policy as JSON")?;

                                    client
                                        .put_bucket_policy()
                                        .bucket(&name)
                                        .policy(policy_json)
                                        .send()
                                        .await
                                        .context("Failed to set bucket policy")?;
                                }

                                // Apply public access block if specified
                                if let Some(public_access_block) = &bucket.public_access_block {
                                    let public_access_block_config =
                                        aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                                            .block_public_acls(public_access_block.block_public_acls)
                                            .ignore_public_acls(public_access_block.ignore_public_acls)
                                            .block_public_policy(public_access_block.block_public_policy)
                                            .restrict_public_buckets(public_access_block.restrict_public_buckets)
                                            .build();

                                    client
                                        .put_public_access_block()
                                        .bucket(&name)
                                        .public_access_block_configuration(public_access_block_config)
                                        .send()
                                        .await
                                        .context("Failed to set public access block")?;
                                }

                                if let Some(acl) = bucket.acl {
                                    let mut grants = Vec::new();
                                    for grant in &acl.grants {
                                        let grantee = aws_sdk_s3::types::Grantee::builder()
                                            .id(&grant.grantee_id)
                                            .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                                            .build()
                                            .context("Failed to build grantee")?;

                                        let permission = aws_sdk_s3::types::Permission::from(grant.permission.as_str());

                                        let grant_obj = aws_sdk_s3::types::Grant::builder()
                                            .grantee(grantee)
                                            .permission(permission)
                                            .build();

                                        grants.push(grant_obj);
                                    }

                                    let owner = aws_sdk_s3::types::Owner::builder().id(&acl.owner_id).build();

                                    let access_control_policy = aws_sdk_s3::types::AccessControlPolicy::builder()
                                        .owner(owner)
                                        .set_grants(Some(grants))
                                        .build();

                                    client
                                        .put_bucket_acl()
                                        .bucket(&name)
                                        .access_control_policy(access_control_policy)
                                        .send()
                                        .await
                                        .context("Failed to set bucket ACL")?;
                                }

                                if bucket.tags.len() > 0 {
                                    let mut tag_set = Vec::new();
                                    let tags_clone = bucket.tags.clone();

                                    if let Some(aws_tags) = Into::<Option<Vec<aws_sdk_s3::types::Tag>>>::into(tags_clone) {
                                        tag_set = aws_tags;
                                    }

                                    let tagging = aws_sdk_s3::types::Tagging::builder()
                                        .set_tag_set(Some(tag_set))
                                        .build()
                                        .context("Failed to build tagging")?;

                                    client
                                        .put_bucket_tagging()
                                        .bucket(&name)
                                        .tagging(tagging)
                                        .send()
                                        .await
                                        .context("Failed to set bucket tags")?;
                                }

                                Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!("Created S3 bucket {name} in region {region}")),
                                })
                            }
                            Err(e) => Err(e.into()),
                        }
                    }
                    S3ConnectorOp::UpdateBucketPolicy(_old_policy, new_policy) => {
                        let client = self.get_or_init_client(&region).await?;

                        match new_policy {
                            Some(policy) => {
                                // Update policy
                                let policy_json =
                                    serde_json::to_string(&policy).context("Failed to serialize bucket policy as JSON")?;

                                client
                                    .put_bucket_policy()
                                    .bucket(&name)
                                    .policy(policy_json)
                                    .send()
                                    .await
                                    .context("Failed to update bucket policy")?;

                                Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!("Updated policy for S3 bucket {name} in region {region}")),
                                })
                            }
                            None => {
                                // Delete policy
                                client
                                    .delete_bucket_policy()
                                    .bucket(&name)
                                    .send()
                                    .await
                                    .context("Failed to delete bucket policy")?;

                                Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!("Deleted policy for S3 bucket {name} in region {region}")),
                                })
                            }
                        }
                    }
                    S3ConnectorOp::UpdateBucketPublicAccessBlock(new_public_access_block) => {
                        let client = self.get_or_init_client(&region).await?;

                        match new_public_access_block {
                            Some(public_access_block) => {
                                // Update public access block configuration
                                let public_access_block_config = aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                                    .block_public_acls(public_access_block.block_public_acls)
                                    .ignore_public_acls(public_access_block.ignore_public_acls)
                                    .block_public_policy(public_access_block.block_public_policy)
                                    .restrict_public_buckets(public_access_block.restrict_public_buckets)
                                    .build();

                                client
                                    .put_public_access_block()
                                    .bucket(&name)
                                    .public_access_block_configuration(public_access_block_config)
                                    .send()
                                    .await
                                    .context("Failed to update public access block")?;

                                Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Updated public access block for S3 bucket {name} in region {region}"
                                    )),
                                })
                            }
                            None => {
                                // Delete public access block configuration
                                client
                                    .delete_public_access_block()
                                    .bucket(&name)
                                    .send()
                                    .await
                                    .context("Failed to delete public access block")?;

                                Ok(OpExecResponse {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Deleted public access block for S3 bucket {name} in region {region}"
                                    )),
                                })
                            }
                        }
                    }
                    S3ConnectorOp::UpdateBucketAcl(_old_acl, new_acl) => {
                        let client = self.get_or_init_client(&region).await?;

                        if let Some(new_acl) = new_acl {
                            // Create grants for new ACL
                            let mut grants = Vec::new();
                            for grant in &new_acl.grants {
                                let grantee = aws_sdk_s3::types::Grantee::builder()
                                    .id(&grant.grantee_id)
                                    .r#type(aws_sdk_s3::types::Type::CanonicalUser)
                                    .build()
                                    .context("Failed to build grantee")?;

                                let permission = aws_sdk_s3::types::Permission::from(grant.permission.as_str());

                                let grant_obj = aws_sdk_s3::types::Grant::builder()
                                    .grantee(grantee)
                                    .permission(permission)
                                    .build();

                                grants.push(grant_obj);
                            }

                            let owner = aws_sdk_s3::types::Owner::builder().id(&new_acl.owner_id).build();

                            let access_control_policy = aws_sdk_s3::types::AccessControlPolicy::builder()
                                .owner(owner)
                                .set_grants(Some(grants))
                                .build();

                            client
                                .put_bucket_acl()
                                .bucket(&name)
                                .access_control_policy(access_control_policy)
                                .send()
                                .await
                                .context("Failed to update bucket ACL")?;
                        }

                        Ok(OpExecResponse {
                            outputs: None,
                            friendly_message: Some(format!("Updated ACL for S3 bucket {name} in region {region}")),
                        })
                    }
                    S3ConnectorOp::UpdateBucketTags(_old_tags, new_tags) => {
                        let client = self.get_or_init_client(&region).await?;

                        if new_tags.len() > 0 {
                            let tagging = aws_sdk_s3::types::Tagging::builder()
                                .set_tag_set(new_tags.into())
                                .build()
                                .context("Failed to build tagging")?;

                            client
                                .put_bucket_tagging()
                                .bucket(&name)
                                .tagging(tagging)
                                .send()
                                .await
                                .context("Failed to update bucket tags")?;
                        } else {
                            // Delete all tags
                            client
                                .delete_bucket_tagging()
                                .bucket(&name)
                                .send()
                                .await
                                .context("Failed to delete bucket tags")?;
                        }

                        Ok(OpExecResponse {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for S3 bucket {name} in region {region}")),
                        })
                    }
                    S3ConnectorOp::DeleteBucket => {
                        let client = self.get_or_init_client(&region).await?;

                        client
                            .delete_bucket()
                            .bucket(&name)
                            .send()
                            .await
                            .context("Failed to delete bucket")?;

                        Ok(OpExecResponse {
                            outputs: None,
                            friendly_message: Some(format!("Deleted S3 bucket {name} in region {region}")),
                        })
                    }
                }
            }
        }
    }
}
