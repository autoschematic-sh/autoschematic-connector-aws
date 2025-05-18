use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::addr::IamResourceAddress;
use anyhow::{bail, Context};
use async_trait::async_trait;
use autoschematic_connector_aws_core::{
    config::AwsConnectorConfig, util::optional_string_from_utf8,
};
use autoschematic_core::{
    connector::{
        Connector, ConnectorOp, ConnectorOutbox, GetResourceOutput, OpExecOutput, OpPlanOutput,
        Resource, ResourceAddress, SkeletonOutput,
    },
    connector_op,
    diag::DiagnosticOutput,
    op_exec_output, skeleton,
    util::{diff_ron_values, ron_check_eq, ron_check_syntax, RON},
};
use op::IamConnectorOp;
use resource::{IamPolicy, IamResource, IamRole, IamUser};

use aws_config::{meta::region::RegionProviderChain, timeout::TimeoutConfig, BehaviorVersion};
use aws_sdk_iam::{config::Region, types::PolicyScopeType};
use tags::{tag_diff, Tags};
use util::{list_attached_role_policies, list_attached_user_policies};

use crate::{
    op, resource, tags,
    util::{self},
};

mod get;
mod list;
mod op_exec;
mod plan;

pub struct IamConnector {
    client: aws_sdk_iam::Client,
    account_id: String,
}

#[async_trait]
impl Connector for IamConnector {
    async fn filter(&self, addr: &Path) -> Result<bool, anyhow::Error> {
        if let Some(_addr) = IamResourceAddress::from_path(addr)? {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn new(
        _name: &str,
        prefix: &Path,
        _outbox: ConnectorOutbox,
    ) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        let config_file = AwsConnectorConfig::try_load(prefix)?;

        let region = RegionProviderChain::first_try(Region::new("global".to_owned()));

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

                if let Some(config_file) = config_file {
                    if config_file.account_id != account_id {
                        bail!("Credentials do not match configured account id: creds = {}, aws/config.ron = {}", account_id, config_file.account_id);
                    }
                }

                Ok(Box::new(IamConnector { client, account_id }))
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
        current: Option<OsString>,
        desired: Option<OsString>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        self.do_plan(
            addr,
            optional_string_from_utf8(current)?,
            optional_string_from_utf8(desired)?,
        )
        .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonOutput>, anyhow::Error> {
        let mut res = Vec::new();

        // IAM User skeleton
        res.push(skeleton!(
            IamResourceAddress::User(String::from("[user_name]")),
            IamResource::User(IamUser {
                attached_policies: vec![
                    String::from("AmazonS3ReadOnlyAccess"),
                    String::from("AmazonEC2ReadOnlyAccess")
                ],
                tags: Tags::default()
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

        let assume_role_policy_value: serde_json::Value =
            serde_json::from_str(assume_role_policy_json)?;
        let assume_role_policy_ron_value: ron::Value =
            RON.from_str(&RON.to_string(&assume_role_policy_value)?)?;

        res.push(skeleton!(
            IamResourceAddress::Role(String::from("[role_name]")),
            IamResource::Role(IamRole {
                attached_policies: vec![
                    String::from("AmazonS3ReadOnlyAccess"),
                    String::from("CloudWatchAgentServerPolicy")
                ],
                assume_role_policy_document: Some(assume_role_policy_ron_value),
                tags: Tags::default()
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
        let policy_document_ron_value: ron::Value =
            RON.from_str(&RON.to_string(&policy_document_value)?)?;

        res.push(skeleton!(
            IamResourceAddress::Policy(String::from("[policy_name]")),
            IamResource::Policy(IamPolicy {
                policy_document: policy_document_ron_value,
                tags: Tags::default()
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &OsStr, b: &OsStr) -> anyhow::Result<bool> {
        let Some(addr) = IamResourceAddress::from_path(addr)? else {
            return Ok(false);
        };
        match addr {
            IamResourceAddress::User(_) => ron_check_eq::<IamUser>(a, b),
            IamResourceAddress::Role(_) => ron_check_eq::<IamRole>(a, b),
            IamResourceAddress::Policy(_) => ron_check_eq::<IamPolicy>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &OsStr) -> Result<DiagnosticOutput, anyhow::Error> {
        let Ok(Some(addr)) = IamResourceAddress::from_path(addr) else {
            return Ok(DiagnosticOutput::default());
        };

        match addr {
            IamResourceAddress::User(_) => ron_check_syntax::<IamUser>(a),
            IamResourceAddress::Role(_) => ron_check_syntax::<IamRole>(a),
            IamResourceAddress::Policy(_) => ron_check_syntax::<IamPolicy>(a),
        }
    }
}
