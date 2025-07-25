use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::addr::S3ResourceAddress;
use crate::config::S3ConnectorConfig;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterResponse, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource,
        ResourceAddress, SkeletonResponse,
    },
    diag::DiagnosticResponse,
    skeleton,
    util::{RON, ron_check_eq, ron_check_syntax},
};

use crate::resource;
use crate::tags::Tags;
use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

#[derive(Default)]
pub struct S3Connector {
    prefix: PathBuf,
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_s3::Client>>>,
    config: Mutex<S3ConnectorConfig>,
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
    async fn new(name: &str, prefix: &Path, outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(S3Connector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config: S3ConnectorConfig = S3ConnectorConfig::try_load(&self.prefix)?.unwrap_or_default();

        *self.config.lock().await = config;
        Ok(())
    }

    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = S3ResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
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
                name:   String::from("[bucket_name]"),
            },
            resource::S3Resource::Bucket(resource::S3Bucket {
                policy: Some(policy_ron_value),
                public_access_block: Some(resource::PublicAccessBlock {
                    block_public_acls: true,
                    ignore_public_acls: true,
                    block_public_policy: true,
                    restrict_public_buckets: true,
                }),
                acl: None,
                tags: Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { .. } => ron_check_eq::<resource::S3Bucket>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticResponse, anyhow::Error> {
        let addr = S3ResourceAddress::from_path(addr)?;

        match addr {
            S3ResourceAddress::Bucket { .. } => ron_check_syntax::<resource::S3Bucket>(a),
        }
    }
}
