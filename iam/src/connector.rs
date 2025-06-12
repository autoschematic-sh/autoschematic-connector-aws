use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{addr::IamResourceAddress, resource::IamGroup};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, DocIdent, FilterOutput, GetDocOutput, GetResourceOutput, OpExecOutput, OpPlanOutput,
        Resource, ResourceAddress, SkeletonOutput,
    },
    diag::DiagnosticOutput,
    skeleton,
    util::{RON, optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use resource::{IamPolicy, IamResource, IamRole, IamUser};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use aws_sdk_iam::config::Region;
use tags::Tags;
use tokio::sync::Mutex;

use crate::{resource, tags};

mod get;
mod get_doc;
mod list;
mod op_exec;
mod plan;

#[derive(Default)]
pub struct IamConnector {
    prefix:     PathBuf,
    client:     Mutex<Option<aws_sdk_iam::Client>>,
    account_id: Mutex<Option<String>>,
}

#[async_trait]
impl Connector for IamConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = IamResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(IamConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let config_file = AwsConnectorConfig::try_load(&self.prefix)?;

        let region = RegionProviderChain::first_try(Region::new("global".to_owned()));

        let config = aws_config::defaults(BehaviorVersion::latest()).region(region).load().await;

        let sts_region = RegionProviderChain::first_try(Region::new("us-east-1".to_owned()));
        let sts_config = aws_config::defaults(BehaviorVersion::latest())
            .region(sts_region)
            .load()
            .await;

        let client = aws_sdk_iam::Client::new(&config);

        let sts_client = aws_sdk_sts::Client::new(&sts_config);

        let caller_identity = sts_client.get_caller_identity().send().await;
        match caller_identity {
            Ok(caller_identity) => {
                let Some(account_id) = caller_identity.account else {
                    bail!("Failed to get current account ID!");
                };

                if let Some(config_account_id) = config_file.account_id {
                    if config_account_id != account_id {
                        bail!(
                            "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                            account_id,
                            config_account_id
                        );
                    }
                }

                *self.client.lock().await = Some(client);
                *self.account_id.lock().await = Some(account_id);

                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
                Err(e.into())
            }
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(addr, optional_string_from_utf8(current)?, optional_string_from_utf8(desired)?)
            .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // IAM User skeleton
        res.push(skeleton!(
            IamResourceAddress::Role {
                path: String::from("/"),
                name: String::from("[user_name]"),
            },
            IamResource::User(IamUser {
                attached_policies: HashSet::from([
                    String::from("AmazonS3ReadOnlyAccess"),
                    String::from("AmazonEC2ReadOnlyAccess")
                ]),
                tags: Tags::default(),
            })
        ));

        // IAM Role skeleton with sample AssumeRolePolicy
        let assume_role_policy_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "ec2.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }"#;

        let assume_role_policy_value: serde_json::Value = serde_json::from_str(assume_role_policy_json)?;
        let assume_role_policy_ron_value: ron::Value = RON.from_str(&RON.to_string(&assume_role_policy_value)?)?;

        res.push(skeleton!(
            IamResourceAddress::Role {
                path: String::from("/"),
                name: String::from("[role_name]"),
            },
            IamResource::Role(IamRole {
                attached_policies: HashSet::from([
                    String::from("AmazonS3ReadOnlyAccess"),
                    String::from("CloudWatchAgentServerPolicy")
                ]),
                assume_role_policy_document: Some(assume_role_policy_ron_value),
                tags: Tags::default(),
            })
        ));

        // IAM Policy skeleton with example policy document
        let policy_document_json = r#"{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::[bucket_name]",
                        "arn:aws:s3:::[bucket_name]/*"
                    ],
                    "Condition": {
                        "StringEquals": {
                            "aws:PrincipalTag/Department": "Data"
                        }
                    }
                }
            ]
        }"#;

        let policy_document_value: serde_json::Value = serde_json::from_str(policy_document_json)?;
        let policy_document_ron_value: ron::Value = RON.from_str(&RON.to_string(&policy_document_value)?)?;

        res.push(skeleton!(
            IamResourceAddress::Policy {
                path: String::from("/"),
                name: String::from("[policy_name]"),
            },
            IamResource::Policy(IamPolicy {
                policy_document: policy_document_ron_value,
                tags: Tags::default(),
            })
        ));

        res.push(skeleton!(
            IamResourceAddress::Group {
                path: "/".into(),
                name: "[group_name]".into(),
            },
            IamResource::Group(IamGroup {
                attached_policies: HashSet::new(),
                users: HashSet::new(),
            })
        ));

        Ok(res)
    }

    async fn get_docstring(&self, addr: &Path, ident: DocIdent) -> anyhow::Result<Option<GetDocOutput>> {
        self.do_get_doc(ident).await
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = IamResourceAddress::from_path(addr)?;

        match addr {
            IamResourceAddress::User { path, name } => ron_check_eq::<IamUser>(a, b),
            IamResourceAddress::Role { path, name } => ron_check_eq::<IamRole>(a, b),
            IamResourceAddress::Group { path, name } => ron_check_eq::<IamGroup>(a, b),
            IamResourceAddress::Policy { path, name } => ron_check_eq::<IamPolicy>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;

        match addr {
            IamResourceAddress::User { path, name } => ron_check_syntax::<IamUser>(a),
            IamResourceAddress::Role { path, name } => ron_check_syntax::<IamRole>(a),
            IamResourceAddress::Group { path, name } => ron_check_syntax::<IamGroup>(a),
            IamResourceAddress::Policy { path, name } => ron_check_syntax::<IamPolicy>(a),
        }
    }
}
