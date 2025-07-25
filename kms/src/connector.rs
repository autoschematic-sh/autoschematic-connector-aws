use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::resource::KmsResource;
use crate::{addr::KmsResourceAddress, tags};
use crate::{config::KmsConnectorConfig, resource};
use crate::{op::KmsConnectorOp, op_impl};
use anyhow::Context;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsServiceConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterResponse, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource,
        ResourceAddress, SkeletonResponse, VirtToPhyResponse,
    },
    connector_op,
    diag::DiagnosticResponse,
    get_resource_response, skeleton,
    util::{RON, diff_ron_values, optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

#[derive(Default)]
pub struct KmsConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_kms::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<KmsConnectorConfig>,
    prefix: PathBuf,
}

impl KmsConnector {
    async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_kms::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(Region::new(region_s.to_owned()));

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
            let client = aws_sdk_kms::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }

    async fn list_region_resources(&self, region: &str, results: &mut Vec<PathBuf>) -> anyhow::Result<()> {
        let client = self.get_or_init_client(region).await?;

        // List Keys
        let list_keys_paginator = client.list_keys().into_paginator().items().send();
        let keys = list_keys_paginator.collect::<Vec<_>>().await;

        for key_result in keys {
            if let Ok(key) = key_result
                && let Some(key_id) = key.key_id {
                    // Add the key
                    results.push(KmsResourceAddress::Key(region.to_string(), key_id.clone()).to_path_buf());

                    // Add key policy
                    results.push(KmsResourceAddress::KeyPolicy(region.to_string(), key_id.clone()).to_path_buf());

                    // Add key rotation status
                    results.push(KmsResourceAddress::KeyRotation(region.to_string(), key_id).to_path_buf());
                }
        }

        // List Aliases
        let list_aliases_paginator = client.list_aliases().into_paginator().items().send();
        let aliases = list_aliases_paginator.collect::<Vec<_>>().await;

        for alias_result in aliases {
            if let Ok(alias) = alias_result
                && let Some(alias_name) = alias.alias_name
                    && let Some(_target_key_id) = alias.target_key_id {
                        // We only care about aliases with a target key
                        results.push(KmsResourceAddress::Alias(region.to_string(), alias_name).to_path_buf());
                    }
        }

        Ok(())
    }
}

#[async_trait]
impl Connector for KmsConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = KmsResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(KmsConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> Result<(), anyhow::Error> {
        let vpc_config = KmsConnectorConfig::try_load(&self.prefix).await?;

        let account_id = vpc_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = vpc_config;
        *self.account_id.lock().await = account_id;

        Ok(())
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        tracing::warn!("KMS List");
        let mut results = Vec::<PathBuf>::new();

        let path_components: Vec<&str> = subpath
            .components()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        let enabled_regions = self.config.lock().await.enabled_regions.clone();

        match &path_components[..] {
            ["aws", "kms", region_name, _rest @ ..] => {
                let region_name = region_name.to_string();
                if enabled_regions.contains(&region_name) {
                    // List specific region
                    self.list_region_resources(&region_name, &mut results).await?;
                }
            }
            _ => {
                // List all enabled regions
                for region_name in &enabled_regions {
                    self.list_region_resources(region_name, &mut results).await?;
                }
            }
        }

        tracing::warn!("KMS List: {:?}", results);
        Ok(results)
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        let addr = KmsResourceAddress::from_path(addr)?;

        match addr {
            KmsResourceAddress::Key(region_name, key_id) => {
                let client = self.get_or_init_client(&region_name).await?;

                // Get the key details
                let describe_key_output = client.describe_key().key_id(&key_id).send().await;

                if let Err(e) = describe_key_output {
                    tracing::warn!("Failed to describe KMS key {}: {}", key_id, e);
                    return Ok(None);
                }

                let describe_key_output = describe_key_output.unwrap();
                let Some(key_metadata) = describe_key_output.key_metadata else {
                    return Ok(None);
                };

                // Get tags
                let list_resource_tags_output = client.list_resource_tags().key_id(&key_id).send().await;

                let tags = if let Ok(tags_output) = list_resource_tags_output {
                    tags_output.tags.into()
                } else {
                    tags::Tags::default()
                };

                // Create the KMS key resource
                let kms_key = resource::KmsKey {
                    description: key_metadata.description.unwrap_or_default(),
                    key_usage: key_metadata
                        .key_usage
                        .map_or_else(|| "ENCRYPT_DECRYPT".to_string(), |usage| usage.as_str().to_string()),
                    customer_master_key_spec: key_metadata
                        .key_spec
                        .map_or_else(|| "SYMMETRIC_DEFAULT".to_string(), |spec| spec.as_str().to_string()),
                    origin: key_metadata
                        .origin
                        .map_or_else(|| "AWS_KMS".to_string(), |origin| origin.as_str().to_string()),
                    multi_region: key_metadata.multi_region.unwrap_or(false),
                    enabled: key_metadata.enabled,
                    tags,
                };

                get_resource_response!(KmsResource::Key(kms_key), [(String::from("key_id"), key_id)])
            }
            KmsResourceAddress::KeyPolicy(region_name, key_id) => {
                let client = self.get_or_init_client(&region_name).await?;

                // Get the key policy
                let get_key_policy_output = client
                    .get_key_policy()
                    .key_id(&key_id)
                    .policy_name("default") // KMS only supports the "default" policy name
                    .send()
                    .await;

                if let Err(e) = get_key_policy_output {
                    tracing::warn!("Failed to get KMS key policy for {}: {}", key_id, e);
                    return Ok(None);
                }

                let get_key_policy_output = get_key_policy_output.unwrap();

                if let Some(policy) = get_key_policy_output.policy {
                    // Convert the policy to a ron::Value
                    let json_val: serde_json::Value = serde_json::from_str(&policy)?;
                    let ron_val: ron::Value = RON.from_str(&RON.to_string(&json_val)?)?;

                    let key_policy = resource::KmsKeyPolicy {
                        policy_document: ron_val,
                    };

                    return Ok(Some(GetResourceResponse {
                        resource_definition: KmsResource::KeyPolicy(key_policy).to_bytes()?,
                        outputs: None,
                    }));
                } else {
                    return Ok(None);
                }
            }
            KmsResourceAddress::Alias(region_name, alias_name) => {
                let client = self.get_or_init_client(&region_name).await?;

                // Get the alias
                let list_aliases_output = client.list_aliases().send().await;

                if let Err(e) = list_aliases_output {
                    tracing::warn!("Failed to list KMS aliases: {}", e);
                    return Ok(None);
                }

                let list_aliases_output = list_aliases_output.unwrap();
                let aliases = list_aliases_output.aliases.unwrap_or_default();

                // Find the specific alias
                for alias in aliases {
                    if let Some(current_alias_name) = &alias.alias_name
                        && current_alias_name == &alias_name
                            && let Some(target_key_id) = alias.target_key_id {
                                // Get tags (tags are on the key, not the alias in KMS)
                                let list_resource_tags_output = client.list_resource_tags().key_id(&target_key_id).send().await;

                                let tags = if let Ok(tags_output) = list_resource_tags_output {
                                    tags_output.tags.into()
                                } else {
                                    tags::Tags::default()
                                };

                                let kms_alias = resource::KmsAlias { target_key_id, tags };

                                return Ok(Some(GetResourceResponse {
                                    resource_definition: KmsResource::Alias(kms_alias).to_bytes()?,
                                    outputs: None,
                                }));
                            }
                }

                return Ok(None);
            }
            KmsResourceAddress::KeyRotation(region_name, key_id) => {
                let client = self.get_or_init_client(&region_name).await?;

                // Get the key rotation status
                let get_key_rotation_status_output = client.get_key_rotation_status().key_id(&key_id).send().await;

                if let Err(e) = get_key_rotation_status_output {
                    tracing::warn!("Failed to get KMS key rotation status for {}: {}", key_id, e);
                    return Ok(None);
                }

                let get_key_rotation_status_output = get_key_rotation_status_output.unwrap();
                let key_rotation_enabled = get_key_rotation_status_output.key_rotation_enabled;

                let key_rotation = resource::KmsKeyRotation {
                    enabled: key_rotation_enabled,
                };

                return Ok(Some(GetResourceResponse {
                    resource_definition: KmsResource::KeyRotation(key_rotation).to_bytes()?,
                    outputs: None,
                }));
            }
        }
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let addr = KmsResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            KmsResourceAddress::Key(region, key_id) => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_key)) => {
                    let new_key: resource::KmsKey = RON.from_str(&new_key)?;
                    Ok(vec![connector_op!(
                        KmsConnectorOp::CreateKey(new_key),
                        format!("Create new KMS key in region {}", region)
                    )])
                }
                (Some(_old_key), None) => Ok(vec![connector_op!(
                    KmsConnectorOp::DeleteKey,
                    format!("Delete KMS key {} in region {}", key_id, region)
                )]),
                (Some(old_key), Some(new_key)) => {
                    let old_key: resource::KmsKey = RON.from_str(&old_key)?;
                    let new_key: resource::KmsKey = RON.from_str(&new_key)?;
                    let mut ops = Vec::new();

                    // Check for description changes
                    if old_key.description != new_key.description {
                        ops.push(connector_op!(
                            KmsConnectorOp::UpdateKeyDescription(old_key.description, new_key.description),
                            format!("Update description for KMS key {} in region {}", key_id, region)
                        ));
                    }

                    // Check for tags changes
                    if old_key.tags != new_key.tags {
                        let diff = diff_ron_values(&old_key.tags, &new_key.tags).unwrap_or_default();
                        ops.push(connector_op!(
                            KmsConnectorOp::UpdateKeyTags(old_key.tags, new_key.tags),
                            format!("Update tags for KMS key {} in region {}\n{}", key_id, region, diff)
                        ));
                    }

                    // Check for enabled state changes
                    if old_key.enabled != new_key.enabled {
                        if new_key.enabled {
                            ops.push(connector_op!(
                                KmsConnectorOp::EnableKey,
                                format!("Enable KMS key {} in region {}", key_id, region)
                            ));
                        } else {
                            ops.push(connector_op!(
                                KmsConnectorOp::DisableKey,
                                format!("Disable KMS key {} in region {}", key_id, region)
                            ));
                        }
                    }

                    Ok(ops)
                }
            },
            KmsResourceAddress::KeyPolicy(region, key_id) => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_policy)) => {
                    // Creating a new policy for an existing key
                    let new_policy: resource::KmsKeyPolicy = RON.from_str(&new_policy)?;
                    let dummy_policy = resource::KmsKeyPolicy {
                        policy_document: ron::Value::Map(ron::Map::new()),
                    };

                    Ok(vec![connector_op!(
                        KmsConnectorOp::UpdateKeyPolicy(dummy_policy, new_policy),
                        format!("Create policy for KMS key {} in region {}", key_id, region)
                    )])
                }
                (Some(_old_policy), None) => {
                    // Removing a policy is not directly supported by KMS, so we'll use a dummy empty policy
                    let dummy_old_policy: resource::KmsKeyPolicy = resource::KmsKeyPolicy {
                        policy_document: ron::Value::Map(ron::Map::new()),
                    };
                    let dummy_new_policy: resource::KmsKeyPolicy = resource::KmsKeyPolicy {
                        policy_document: ron::Value::Map(ron::Map::new()),
                    };

                    Ok(vec![connector_op!(
                        KmsConnectorOp::UpdateKeyPolicy(dummy_old_policy, dummy_new_policy),
                        format!("Remove policy from KMS key {} in region {}", key_id, region)
                    )])
                }
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: resource::KmsKeyPolicy = RON.from_str(&old_policy)?;
                    let new_policy: resource::KmsKeyPolicy = RON.from_str(&new_policy)?;

                    if old_policy.policy_document != new_policy.policy_document {
                        let diff =
                            diff_ron_values(&old_policy.policy_document, &new_policy.policy_document).unwrap_or_default();

                        Ok(vec![connector_op!(
                            KmsConnectorOp::UpdateKeyPolicy(old_policy, new_policy),
                            format!("Update policy for KMS key {} in region {}\n{}", key_id, region, diff)
                        )])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            KmsResourceAddress::Alias(region, alias_name) => match (current, desired) {
                (None, None) => Ok(Vec::new()),
                (None, Some(new_alias)) => {
                    let new_alias: resource::KmsAlias = RON.from_str(&new_alias)?;

                    Ok(vec![connector_op!(
                        KmsConnectorOp::CreateAlias(new_alias),
                        format!("Create new KMS alias {} in region {}", alias_name, region)
                    )])
                }
                (Some(_old_alias), None) => Ok(vec![connector_op!(
                    KmsConnectorOp::DeleteAlias,
                    format!("Delete KMS alias {} in region {}", alias_name, region)
                )]),
                (Some(old_alias), Some(new_alias)) => {
                    let old_alias: resource::KmsAlias = RON.from_str(&old_alias)?;
                    let new_alias: resource::KmsAlias = RON.from_str(&new_alias)?;

                    if old_alias.target_key_id != new_alias.target_key_id {
                        Ok(vec![connector_op!(
                            KmsConnectorOp::UpdateAlias(new_alias.target_key_id.clone()),
                            format!(
                                "Update KMS alias {} to point to key {} in region {}",
                                alias_name, new_alias.target_key_id, region
                            )
                        )])
                    } else {
                        Ok(Vec::new())
                    }
                }
            },
            KmsResourceAddress::KeyRotation(region, key_id) => {
                match (current, desired) {
                    (None, None) => Ok(Vec::new()),
                    (None, Some(new_rotation)) => {
                        let new_rotation: resource::KmsKeyRotation = RON.from_str(&new_rotation)?;

                        if new_rotation.enabled {
                            Ok(vec![connector_op!(
                                KmsConnectorOp::EnableKeyRotation,
                                format!("Enable automatic key rotation for KMS key {} in region {}", key_id, region)
                            )])
                        } else {
                            Ok(vec![connector_op!(
                                KmsConnectorOp::DisableKeyRotation,
                                format!("Disable automatic key rotation for KMS key {} in region {}", key_id, region)
                            )])
                        }
                    }
                    (Some(_old_rotation), None) => {
                        // Default is to disable rotation when removing the configuration
                        Ok(vec![connector_op!(
                            KmsConnectorOp::DisableKeyRotation,
                            format!("Disable automatic key rotation for KMS key {} in region {}", key_id, region)
                        )])
                    }
                    (Some(old_rotation), Some(new_rotation)) => {
                        let old_rotation: resource::KmsKeyRotation = RON.from_str(&old_rotation)?;
                        let new_rotation: resource::KmsKeyRotation = RON.from_str(&new_rotation)?;

                        if old_rotation.enabled != new_rotation.enabled {
                            if new_rotation.enabled {
                                Ok(vec![connector_op!(
                                    KmsConnectorOp::EnableKeyRotation,
                                    format!("Enable automatic key rotation for KMS key {} in region {}", key_id, region)
                                )])
                            } else {
                                Ok(vec![connector_op!(
                                    KmsConnectorOp::DisableKeyRotation,
                                    format!("Disable automatic key rotation for KMS key {} in region {}", key_id, region)
                                )])
                            }
                        } else {
                            Ok(Vec::new())
                        }
                    }
                }
            }
        }
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        let addr = KmsResourceAddress::from_path(addr)?;
        let op = KmsConnectorOp::from_str(op)?;

        match addr {
            KmsResourceAddress::Key(region, key_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    KmsConnectorOp::CreateKey(key) => op_impl::create_key(&client, &key).await,
                    KmsConnectorOp::UpdateKeyDescription(_, description) => {
                        op_impl::update_key_description(&client, &key_id, &description).await
                    }
                    KmsConnectorOp::UpdateKeyTags(old_tags, new_tags) => {
                        op_impl::update_key_tags(&client, &key_id, &old_tags, &new_tags).await
                    }
                    KmsConnectorOp::EnableKey => op_impl::enable_key(&client, &key_id).await,
                    KmsConnectorOp::DisableKey => op_impl::disable_key(&client, &key_id).await,
                    KmsConnectorOp::DeleteKey => op_impl::delete_key(&client, &key_id).await,
                    _ => bail!("Invalid operation for KMS key: {:?}", op),
                }
            }
            KmsResourceAddress::KeyPolicy(region, key_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    KmsConnectorOp::UpdateKeyPolicy(_, new_policy) => {
                        op_impl::update_key_policy(&client, &key_id, &new_policy).await
                    }
                    _ => bail!("Invalid operation for KMS key policy: {:?}", op),
                }
            }
            KmsResourceAddress::Alias(region, alias_name) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    KmsConnectorOp::CreateAlias(alias) => op_impl::create_alias(&client, &alias_name, &alias).await,
                    KmsConnectorOp::UpdateAlias(target_key_id) => {
                        op_impl::update_alias(&client, &alias_name, &target_key_id).await
                    }
                    KmsConnectorOp::DeleteAlias => op_impl::delete_alias(&client, &alias_name).await,
                    _ => bail!("Invalid operation for KMS alias: {:?}", op),
                }
            }
            KmsResourceAddress::KeyRotation(region, key_id) => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    KmsConnectorOp::EnableKeyRotation => op_impl::enable_key_rotation(&client, &key_id).await,
                    KmsConnectorOp::DisableKeyRotation => op_impl::disable_key_rotation(&client, &key_id).await,
                    _ => bail!("Invalid operation for KMS key rotation: {:?}", op),
                }
            }
        }
    }

    async fn addr_virt_to_phy(&self, addr: &Path) -> anyhow::Result<VirtToPhyResponse> {
        let Ok(addr) = KmsResourceAddress::from_path(addr) else {
            return Ok(VirtToPhyResponse::NotPresent);
        };

        match &addr {
            KmsResourceAddress::Key(region, _key_id) => {
                if let Some(key_id) = addr.get_output(&self.prefix, "key_id")? {
                    Ok(VirtToPhyResponse::Present(
                        KmsResourceAddress::Key(region.into(), key_id).to_path_buf(),
                    ))
                } else {
                    Ok(VirtToPhyResponse::NotPresent)
                }
            },
            _ => Ok(VirtToPhyResponse::Null(addr.to_path_buf())),
        }
    }

    async fn addr_phy_to_virt(&self, addr: &Path) -> anyhow::Result<Option<PathBuf>> {
        let Ok(addr) = KmsResourceAddress::from_path(addr) else {
            return Ok(None);
        };

        match &addr {
            KmsResourceAddress::Key(_, _) => {
                if let Some(key_addr) = addr.phy_to_virt(&self.prefix)? {
                    return Ok(Some(key_addr.to_path_buf()));
                }
            }
            _ => {
                return Ok(Some(addr.to_path_buf()));
            }
        }
        Ok(Some(addr.to_path_buf()))
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        // KMS Key skeleton
        res.push(skeleton!(
            KmsResourceAddress::Key(String::from("[region]"), String::from("[key_id]")),
            KmsResource::Key(resource::KmsKey {
                description: String::from("Example encryption key for secure data"),
                key_usage: String::from("ENCRYPT_DECRYPT"),
                customer_master_key_spec: String::from("SYMMETRIC_DEFAULT"),
                origin: String::from("AWS_KMS"),
                multi_region: false,
                enabled: true,
                tags: tags::Tags::default(),
            })
        ));

        // KMS Key Policy skeleton
        let key_policy_json = r#"{
            "Version": "2012-10-17",
            "Id": "key-default-1",
            "Statement": [
                {
                    "Sid": "Enable IAM User Permissions",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::[account_id]:root"
                    },
                    "Action": "kms:*",
                    "Resource": "*"
                },
                {
                    "Sid": "Allow use of the key",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "arn:aws:iam::[account_id]:role/[role_name]"
                        ]
                    },
                    "Action": [
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"
                    ],
                    "Resource": "*"
                },
                {
                    "Sid": "Allow attachment of persistent resources",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [
                            "arn:aws:iam::[account_id]:role/[role_name]"
                        ]
                    },
                    "Action": [
                        "kms:CreateGrant",
                        "kms:ListGrants",
                        "kms:RevokeGrant"
                    ],
                    "Resource": "*",
                    "Condition": {
                        "Bool": {
                            "kms:GrantIsForAWSResource": "true"
                        }
                    }
                }
            ]
        }"#;

        let policy_json_value: serde_json::Value = serde_json::from_str(key_policy_json)?;
        let policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&policy_json_value)?)?;

        res.push(skeleton!(
            KmsResourceAddress::KeyPolicy(String::from("[region]"), String::from("[key_id]")),
            KmsResource::KeyPolicy(resource::KmsKeyPolicy {
                policy_document: policy_ron_value,
            })
        ));

        // KMS Alias skeleton
        res.push(skeleton!(
            KmsResourceAddress::Alias(String::from("[region]"), String::from("alias/[alias_name]")),
            KmsResource::Alias(resource::KmsAlias {
                target_key_id: String::from("[key_id]"),
                tags: tags::Tags::default(),
            })
        ));

        // KMS Key Rotation skeleton
        res.push(skeleton!(
            KmsResourceAddress::KeyRotation(String::from("[region]"), String::from("[key_id]")),
            KmsResource::KeyRotation(resource::KmsKeyRotation { enabled: true })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = KmsResourceAddress::from_path(addr)?;

        match addr {
            KmsResourceAddress::Key(_, _) => ron_check_eq::<resource::KmsKey>(a, b),
            KmsResourceAddress::KeyPolicy(_, _) => ron_check_eq::<resource::KmsKeyPolicy>(a, b),
            KmsResourceAddress::Alias(_, _) => ron_check_eq::<resource::KmsAlias>(a, b),
            KmsResourceAddress::KeyRotation(_, _) => ron_check_eq::<resource::KmsKeyRotation>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = KmsResourceAddress::from_path(addr)?;

        match addr {
            KmsResourceAddress::Key(_, _) => ron_check_syntax::<resource::KmsKey>(a),
            KmsResourceAddress::KeyPolicy(_, _) => ron_check_syntax::<resource::KmsKeyPolicy>(a),
            KmsResourceAddress::Alias(_, _) => ron_check_syntax::<resource::KmsAlias>(a),
            KmsResourceAddress::KeyRotation(_, _) => ron_check_syntax::<resource::KmsKeyRotation>(a),
        }
    }
}
