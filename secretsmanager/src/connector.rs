pub use crate::addr::SecretsManagerResourceAddress;
pub use crate::op::SecretsManagerConnectorOp;

use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::{
    config::SecretsManagerConnectorConfig,
    resource::{Secret, SecretsManagerResource},
    tags,
};
use anyhow::{Context, bail};
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource,
        ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
    util::{RON, ron_check_eq, ron_check_syntax},
};
use autoschematic_core::{get_resource_output, skeleton};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use serde_json;
use tokio::sync::Mutex;

use crate::resource;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use tags::Tags;

pub mod get;
pub mod list;
pub mod op_exec;
pub mod plan;

// Helper function to get a secret
async fn get_secret(client: &aws_sdk_secretsmanager::Client, secret_name: &str) -> anyhow::Result<(resource::Secret, String)> {
    // Describe the secret to get its metadata
    let describe_resp = client.describe_secret().secret_id(secret_name).send().await?;

    // Get tags if they exist
    let tags = describe_resp.tags.map(|t| Tags::from(&t[..])).unwrap_or_default();

    let policy_resp = client
        .get_resource_policy()
        .secret_id(secret_name)
        .send()
        .await
        .context("Failed to get resource policy")?;

    let policy_str = policy_resp.resource_policy.context("Resource policy not found")?;

    // Parse the policy JSON into a ron::Value
    let policy_json: serde_json::Value = serde_json::from_str(&policy_str).context("Failed to parse policy JSON")?;

    let policy_value = RON
        .from_str(&policy_json.to_string())
        .context("Failed to convert policy to RON value")?;

    // Create the Secret struct
    let secret = resource::Secret {
        description: describe_resp.description.clone(),
        kms_key_id: describe_resp.kms_key_id.clone(),
        secret_ref: None, // By default, we don't include the secret value for security reasons
        tags,
        policy_document: policy_value,
    };

    Ok((secret, describe_resp.arn.unwrap_or_default()))
}

#[derive(Default)]
pub struct SecretsManagerConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_secretsmanager::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<SecretsManagerConnectorConfig>,
    prefix: PathBuf,
}

impl SecretsManagerConnector {
    pub async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_secretsmanager::Client>> {
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
            let client = aws_sdk_secretsmanager::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        };

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for SecretsManagerConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(SecretsManagerConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config_file = AwsConnectorConfig::try_load(&self.prefix)?;

        let region_str = "us-east-1";
        let region = RegionProviderChain::first_try(Region::new(region_str.to_owned()));

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

        tracing::warn!("SecretsManagerConnector::new()!");

        // Get account ID from STS
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(RegionProviderChain::first_try(Region::new("us-east-1".to_owned())))
            .load()
            .await;

        let sts_client = aws_sdk_sts::Client::new(&sts_config);
        let caller_identity = sts_client.get_caller_identity().send().await;

        match caller_identity {
            Ok(caller_identity) => {
                let Some(account_id) = caller_identity.account else {
                    bail!("Failed to get current account ID!");
                };

                if let Some(config_file) = config_file {
                    if config_file.account_id != account_id {
                        bail!(
                            "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                            account_id,
                            config_file.account_id
                        );
                    }
                }

                let secrets_config: SecretsManagerConnectorConfig =
                    SecretsManagerConnectorConfig::try_load(&self.prefix)?.unwrap_or_default();

                *self.client_cache.lock().await = HashMap::new();
                *self.config.lock().await = secrets_config;
                *self.account_id.lock().await = account_id;
                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
                Err(e.into())
            }
        }
    }

    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = SecretsManagerResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = SecretsManagerResourceAddress::from_path(addr)?;

        match addr {
            SecretsManagerResourceAddress::Secret { region, name } => {
                let client = self.get_or_init_client(&region).await?;
                match get_secret(&client, &name).await {
                    Ok((secret, arn)) => {
                        return get_resource_output!(
                            SecretsManagerResource::Secret(secret),
                            [(String::from("arn"), Some(arn))]
                        );
                    }
                    Err(_) => Ok(None),
                }
            }
        }
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // Create a default JSON policy structure
        let default_policy_json = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "arn:aws:iam::[account_id]:role/[role_name]"
                    },
                    "Action": "secretsmanager:GetSecretValue",
                    "Resource": "*"
                }
            ]
        });

        // Convert JSON to RON
        let default_policy = match RON.from_str(&default_policy_json.to_string()) {
            Ok(val) => val,
            Err(_) => ron::Value::Option(None), // Fallback
        };
        //
        // Add skeleton for a secret
        res.push(skeleton!(
            SecretsManagerResourceAddress::Secret {
                region: String::from("[region]"),
                name: String::from("[secret_name]")
            },
            SecretsManagerResource::Secret(Secret {
                description: Some(String::from("Example secret description")),
                kms_key_id: None,
                secret_ref: Some(String::from("secret://aws/secretmanager/some/secret.sealed")),
                tags: Tags::default(),
                policy_document: default_policy
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &OsStr, b: &OsStr) -> anyhow::Result<bool> {
        let addr = SecretsManagerResourceAddress::from_path(addr)?;

        match addr {
            SecretsManagerResourceAddress::Secret { region, name } => ron_check_eq::<resource::Secret>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &OsStr) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = SecretsManagerResourceAddress::from_path(addr)?;

        match addr {
            SecretsManagerResourceAddress::Secret { region, name } => ron_check_syntax::<resource::Secret>(a),
        }
    }
}
