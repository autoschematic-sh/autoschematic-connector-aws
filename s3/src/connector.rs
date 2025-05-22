use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::addr::S3ResourceAddress;
use crate::config::S3ConnectorConfig;
use crate::op::S3ConnectorOp;
use crate::util;
use anyhow::{Context, bail};
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress,
        SkeletonOutput,
    },
    connector_op,
    diag::DiagnosticOutput,
    skeleton,
    util::{diff_ron_values, optional_string_from_utf8, ron_check_eq, ron_check_syntax, RON},
};

use crate::resource;
use crate::tags::Tags;
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

pub struct S3Connector {
    client_cache: tokio::sync::Mutex<HashMap<String, Arc<aws_sdk_s3::Client>>>,
    config: S3ConnectorConfig,
}

impl S3Connector {
    async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_s3::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(aws_config::Region::new(region_s.to_owned()));

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .timeout_config(
                    TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(30))
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_secs(30))
                        .read_timeout(Duration::from_secs(30))
                        .build(),
                )
                .load()
                .await;
            let client = aws_sdk_s3::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for S3Connector {
    async fn new(name: &str, prefix: &Path, outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        let config: S3ConnectorConfig = S3ConnectorConfig::try_load(prefix)?.unwrap_or_default();
        tracing::warn!("Successfully created S3Connector");
        Ok(Box::new(S3Connector {
            client_cache: Mutex::new(HashMap::new()),
            config,
        }))
    }

    async fn filter(&self, addr: &Path) -> Result<bool, anyhow::Error> {
        if let Ok(_addr) = S3ResourceAddress::from_path(addr) {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        tracing::warn!("List: ");
        let mut results = Vec::<PathBuf>::new();

        let path_components: Vec<&str> = subpath
            .components()
            .into_iter()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match &path_components[..] {
            ["aws", "s3", region_name, prefix @ ..] => {
                let region_name = region_name.to_string();
                if self.config.enabled_regions.contains(&region_name) {
                    let prefix = if prefix.len() > 0 { Some(prefix.join("/")) } else { None };
                    let client = self.get_or_init_client(&region_name).await.unwrap();
                    let bucket_names = util::list_buckets(client, &region_name, prefix).await?;
                    for bucket_name in bucket_names {
                        results.push(
                            S3ResourceAddress::Bucket {
                                region: region_name.clone(),
                                name: bucket_name,
                            }
                            .to_path_buf(),
                        );
                    }
                } else {
                    return Ok(Vec::new());
                }
            }

            _ => {
                for region_name in &self.config.enabled_regions {
                    let client = self.get_or_init_client(&region_name).await.unwrap();
                    let bucket_names = util::list_buckets(client, &region_name, None).await?;
                    for bucket_name in bucket_names {
                        results.push(
                            S3ResourceAddress::Bucket {
                                region: region_name.clone(),
                                name: bucket_name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        tracing::warn!("List: {:?}", results);
        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                let tagging_output = client.get_bucket_tagging().bucket(&name).send().await;

                let policy_output = client.get_bucket_policy().bucket(&name).send().await;

                let acl_output = client.get_bucket_acl().bucket(&name).send().await?;
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
                    match public_access_block_output.public_access_block_configuration {
                        Some(conf) => {
                            Some(resource::PublicAccessBlock {
                                block_public_acls: conf.block_public_acls.unwrap_or(false),
                                ignore_public_acls: conf.ignore_public_acls.unwrap_or(false),
                                block_public_policy: conf.block_public_policy.unwrap_or(false),
                                restrict_public_buckets: conf.restrict_public_buckets.unwrap_or(false),
                            })
                        }
                        None => None,
                    }
                } else {
                    None
                };

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

                let acl = resource::Acl {
                    owner_id: owner.id.unwrap_or_default(),
                    grants,
                };

                let tags = if let Ok(tagging_output) = tagging_output {
                    tagging_output.tag_set.into()
                } else {
                    Tags::default()
                };

                let bucket = resource::S3Bucket {
                    policy: policy,
                    public_access_block: public_access_block,
                    acl: acl,
                    tags: tags,
                };

                return Ok(Some(GetResourceOutput {
                    resource_definition: resource::S3Resource::Bucket(bucket).to_os_string()?,
                    outputs: None,
                }));
            }
        }
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => {
                match (current, desired) {
                    (None, None) => Ok(Vec::new()),
                    (None, Some(new_bucket)) => {
                        let new_bucket: resource::S3Bucket = RON.from_str(&new_bucket)?;
                        Ok(vec![connector_op!(
                            S3ConnectorOp::CreateBucket(new_bucket),
                            format!("Create new bucket {} in region {}", name, region)
                        )])
                    }

                    (Some(_old_bucket), None) => {
                        Ok(vec![connector_op!(
                            S3ConnectorOp::DeleteBucket,
                            format!("DELETE bucket {} in region {}", name, region)
                        )])
                    }
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
                }
            }
        }
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;
        let op = S3ConnectorOp::from_str(op)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => {
                match op {
                    S3ConnectorOp::CreateBucket(bucket) => {
                        let client = self.get_or_init_client(&region).await?;

                        // Create the bucket
                        match client.create_bucket().bucket(&name).send().await {
                            Ok(_) => {
                                // Apply policy if specified
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

                                // Apply ACL
                                let mut grants = Vec::new();
                                for grant in &bucket.acl.grants {
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

                                let owner = aws_sdk_s3::types::Owner::builder().id(&bucket.acl.owner_id).build();

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

                                // Apply tags
                                if bucket.tags.len() > 0 {
                                    // Use bucket.tags.clone() to create a new copy we can convert
                                    let mut tag_set = Vec::new();
                                    let tags_clone = bucket.tags.clone();

                                    // Convert tags to AWS format - manually since we can't access .0 directly
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

                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!("Created S3 bucket {} in region {}", name, region)),
                                })
                            }
                            Err(e) if e.to_string().contains("BucketAlreadyOwnedByYou") => {
                                // Bucket already exists and is owned by the same AWS account, which is fine
                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "S3 bucket {} in region {} already exists and is owned by you",
                                        name, region
                                    )),
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

                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Updated policy for S3 bucket {} in region {}",
                                        name, region
                                    )),
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

                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Deleted policy for S3 bucket {} in region {}",
                                        name, region
                                    )),
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

                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Updated public access block for S3 bucket {} in region {}",
                                        name, region
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

                                Ok(OpExecOutput {
                                    outputs: None,
                                    friendly_message: Some(format!(
                                        "Deleted public access block for S3 bucket {} in region {}",
                                        name, region
                                    )),
                                })
                            }
                        }
                    }
                    S3ConnectorOp::UpdateBucketAcl(old_acl, new_acl) => {
                        let client = self.get_or_init_client(&region).await?;

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

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated ACL for S3 bucket {} in region {}", name, region)),
                        })
                    }
                    S3ConnectorOp::UpdateBucketTags(old_tags, new_tags) => {
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

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for S3 bucket {} in region {}", name, region)),
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

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Deleted S3 bucket {} in region {}", name, region)),
                        })
                    }
                }
            }
            _ => bail!("Invalid address {:?} for S3 operation! This is a bug in the connector.", addr),
        }
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // Create an example bucket policy (a simple read-only policy)
        let example_policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": "arn:aws:s3:::[bucket_name]/*",
                    "Condition": {
                        "IpAddress": {
                            "aws:SourceIp": "192.168.0.0/24"
                        }
                    }
                }
            ]
        }"#;

        let policy_value: serde_json::Value = serde_json::from_str(example_policy_json)?;
        let policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&policy_value)?)?;

        res.push(skeleton!(
            S3ResourceAddress::Bucket {
                region: String::from("[region]"),
                name: String::from("[bucket_name]")
            },
            resource::S3Resource::Bucket(resource::S3Bucket {
                policy: Some(policy_ron_value),
                public_access_block: Some(resource::PublicAccessBlock {
                    block_public_acls: true,
                    ignore_public_acls: true,
                    block_public_policy: true,
                    restrict_public_buckets: true
                }),
                acl: resource::Acl {
                    owner_id: String::from("[owner_id]"),
                    grants: vec![resource::Grant {
                        grantee_id: String::from("[grantee_id]"),
                        permission: String::from("READ")
                    }],
                },
                tags: Tags::default()
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &OsStr, b: &OsStr) -> anyhow::Result<bool> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => ron_check_eq::<resource::S3Bucket>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &OsStr) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { region, name } => ron_check_syntax::<resource::S3Bucket>(a),
        }
    }
}
