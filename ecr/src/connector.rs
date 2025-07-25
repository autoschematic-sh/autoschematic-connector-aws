pub use crate::addr::EcrResourceAddress;
pub use crate::op::EcrConnectorOp;
pub use crate::resource::EcrResource;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::FilterResponse,
    skeleton,
    util::{ron_check_eq, ron_check_syntax},
};
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource, ResourceAddress,
        SkeletonResponse,
    },
    diag::DiagnosticResponse,
    util::RON,
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};

use crate::config::EcrConnectorConfig;
use tokio::sync::Mutex;

use crate::resource::{
    EncryptionConfiguration, ImageScanningConfiguration, LifecyclePolicy, PullThroughCacheRule, RegistryPolicy, Repository,
    RepositoryPolicy,
};
use crate::tags::Tags;
use autoschematic_connector_aws_core::config::AwsServiceConfig;

#[derive(Default)]
pub struct EcrConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_ecr::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<EcrConnectorConfig>,
    prefix: PathBuf,
}

impl EcrConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_ecr::Client>> {
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
            let client = aws_sdk_ecr::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for EcrConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = EcrResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(EcrConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> Result<(), anyhow::Error> {
        let ecr_config: EcrConnectorConfig = EcrConnectorConfig::try_load(&self.prefix).await?;

        let account_id = ecr_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = ecr_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        // Repository skeleton
        res.push(skeleton!(
            EcrResourceAddress::Repository {
                region: String::from("[region]"),
                name:   String::from("[repository_name]"),
            },
            EcrResource::Repository(Repository {
                encryption_configuration: Some(EncryptionConfiguration {
                    encryption_type: String::from("AES256"), // or "KMS"
                    kms_key: None,
                }),
                image_tag_mutability: Some(String::from("IMMUTABLE")), // or "MUTABLE"
                image_scanning_configuration: Some(ImageScanningConfiguration { scan_on_push: true }),
                tags: Tags::default(),
            })
        ));

        // Repository policy skeleton with example policy
        let repo_policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowPull",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::[account_id]:role/[role_name]"
                    },
                    "Action": [
                        "ecr:BatchGetImage",
                        "ecr:GetDownloadUrlForLayer"
                    ]
                }
            ]
        }"#;

        let repo_policy_value: serde_json::Value = serde_json::from_str(repo_policy_json)?;
        let repo_policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&repo_policy_value)?)?;

        res.push(skeleton!(
            EcrResourceAddress::RepositoryPolicy {
                region: String::from("[region]"),
                name:   String::from("[repository_name]"),
            },
            EcrResource::RepositoryPolicy(RepositoryPolicy {
                policy_document: repo_policy_ron_value,
            })
        ));

        // Lifecycle policy skeleton with example policy
        let lifecycle_policy_json = r#"{
            "rules": [
                {
                    "rulePriority": 1,
                    "description": "Keep only one untagged image, expire all others",
                    "selection": {
                        "tagStatus": "untagged",
                        "countType": "imageCountMoreThan",
                        "countNumber": 1
                    },
                    "action": {
                        "type": "expire"
                    }
                },
                {
                    "rulePriority": 2,
                    "description": "Keep last 10 images",
                    "selection": {
                        "tagStatus": "any",
                        "countType": "imageCountMoreThan",
                        "countNumber": 10
                    },
                    "action": {
                        "type": "expire"
                    }
                }
            ]
        }"#;

        let lifecycle_policy_value: serde_json::Value = serde_json::from_str(lifecycle_policy_json)?;
        let lifecycle_policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&lifecycle_policy_value)?)?;

        res.push(skeleton!(
            EcrResourceAddress::LifecyclePolicy {
                region: String::from("[region]"),
                name:   String::from("[repository_name]"),
            },
            EcrResource::LifecyclePolicy(LifecyclePolicy {
                lifecycle_policy_text: lifecycle_policy_ron_value,
            })
        ));

        // Registry policy skeleton with example policy
        let registry_policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowReplication",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ecr.amazonaws.com"
                    },
                    "Action": [
                        "ecr:CreateRepository",
                        "ecr:ReplicateImage"
                    ],
                    "Resource": [
                        "arn:aws:ecr:[region]:[account_id]:repository/*"
                    ],
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": "[source_account_id]"
                        }
                    }
                }
            ]
        }"#;

        let registry_policy_value: serde_json::Value = serde_json::from_str(registry_policy_json)?;
        let registry_policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&registry_policy_value)?)?;

        res.push(skeleton!(
            EcrResourceAddress::RegistryPolicy {
                region: String::from("[region]"),
            },
            EcrResource::RegistryPolicy(RegistryPolicy {
                policy_document: registry_policy_ron_value,
            })
        ));

        // Pull Through Cache Rule skeleton
        res.push(skeleton!(
            EcrResourceAddress::PullThroughCacheRule {
                region: String::from("[region]"),
                prefix: String::from("[ecr_repository_prefix]"),
            },
            EcrResource::PullThroughCacheRule(PullThroughCacheRule {
                upstream_registry_url: String::from("public.ecr.aws"), // Example for AWS public registry
                credential_arn: None, // Optional: "arn:aws:secretsmanager:[region]:[account_id]:secret:[secret_name]"
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = EcrResourceAddress::from_path(addr)?;
        match addr {
            EcrResourceAddress::Repository { region: _, name: _ } => ron_check_eq::<Repository>(a, b),
            EcrResourceAddress::RepositoryPolicy { region: _, name: _ } => ron_check_eq::<RepositoryPolicy>(a, b),
            EcrResourceAddress::LifecyclePolicy { region, name } => ron_check_eq::<LifecyclePolicy>(a, b),
            EcrResourceAddress::RegistryPolicy { region } => ron_check_eq::<RegistryPolicy>(a, b),
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => ron_check_eq::<PullThroughCacheRule>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = EcrResourceAddress::from_path(addr)?;

        match addr {
            EcrResourceAddress::Repository { region: _, name: _ } => ron_check_syntax::<Repository>(a),
            EcrResourceAddress::RepositoryPolicy { region: _, name: _ } => ron_check_syntax::<RepositoryPolicy>(a),
            EcrResourceAddress::LifecyclePolicy { region, name } => ron_check_syntax::<LifecyclePolicy>(a),
            EcrResourceAddress::RegistryPolicy { region } => ron_check_syntax::<RegistryPolicy>(a),
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => ron_check_syntax::<PullThroughCacheRule>(a),
        }
    }
}
