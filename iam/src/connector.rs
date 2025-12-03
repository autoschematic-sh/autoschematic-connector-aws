use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    addr::IamResourceAddress,
    resource::IamGroup,
    task::{IamTask, IamTaskAddress},
};
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, DocIdent, FilterResponse, GetDocResponse, GetResourceResponse, OpExecResponse,
        PlanResponseElement, Resource, ResourceAddress, SkeletonResponse, TaskExecResponse,
    },
    diag::DiagnosticResponse,
    doc_dispatch,
    skeleton,
    util::{RON, optional_string_from_utf8, ron_check_eq, ron_check_syntax},
};
use resource::{IamPolicy, IamResource, IamRole, IamUser};

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_iam::config::Region;
use tags::Tags;
use tokio::sync::RwLock;

use crate::{resource, tags};

mod get;
mod list;
mod op_exec;
mod plan;

#[derive(Default)]
pub struct IamConnector {
    prefix: PathBuf,
    client: RwLock<Option<Arc<aws_sdk_iam::Client>>>,
    account_id: RwLock<Option<String>>,
}

#[async_trait]
impl Connector for IamConnector {
    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(IamConnector {
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

                if let Some(config_account_id) = config_file.account_id
                    && config_account_id != account_id
                {
                    bail!(
                        "Credentials do not match configured account id: creds = {}, aws/config.ron = {}",
                        account_id,
                        config_account_id
                    );
                }

                *self.client.write().await = Some(Arc::new(client));
                *self.account_id.write().await = Some(account_id);

                Ok(())
            }
            Err(e) => {
                tracing::error!("Failed to call sts:GetCallerIdentity: {}", e);
                Err(e.into())
            }
        }
    }

    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = IamResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn subpaths(&self) -> Result<Vec<PathBuf>, anyhow::Error> {
        Ok(vec![
            PathBuf::from("aws/iam/users"),
            PathBuf::from("aws/iam/roles"),
            PathBuf::from("aws/iam/groups"),
            PathBuf::from("aws/iam/policies"),
        ])
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
        self.do_plan(addr, optional_string_from_utf8(current)?, optional_string_from_utf8(desired)?)
            .await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        // IAM User skeleton
        res.push(skeleton!(
            IamResourceAddress::User {
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
                attached_policies: HashSet::from([]),
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

    async fn get_docstring(&self, _addr: &Path, ident: DocIdent) -> anyhow::Result<Option<GetDocResponse>> {
        doc_dispatch!(ident, [IamRole])
        // doc_dispatch!(ident, [IamUser, IamRole, IamGroup, IamPolicy])
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = IamResourceAddress::from_path(addr)?;

        match addr {
            IamResourceAddress::User { .. } => ron_check_eq::<IamUser>(a, b),
            IamResourceAddress::Role { .. } => ron_check_eq::<IamRole>(a, b),
            IamResourceAddress::Group { .. } => ron_check_eq::<IamGroup>(a, b),
            IamResourceAddress::Policy { .. } => ron_check_eq::<IamPolicy>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;

        match addr {
            IamResourceAddress::User { .. } => ron_check_syntax::<IamUser>(a),
            IamResourceAddress::Role { .. } => ron_check_syntax::<IamRole>(a),
            IamResourceAddress::Group { .. } => ron_check_syntax::<IamGroup>(a),
            IamResourceAddress::Policy { .. } => ron_check_syntax::<IamPolicy>(a),
        }
    }

    async fn task_exec(
        &self,
        addr: &Path,
        body: Vec<u8>,

        _arg: Option<Vec<u8>>,
        _state: Option<Vec<u8>>,
    ) -> anyhow::Result<TaskExecResponse> {
        let mut res = TaskExecResponse::default();

        let addr = IamTaskAddress::from_path(addr)?;

        let Some(ref client) = *self.client.read().await else {
            bail!("No client")
        };

        let task = IamTask::from_bytes(&addr, &body)?;
        match task {
            IamTask::RotateCredential(rotate_credential) => {
                for cred in rotate_credential.credentials {
                    let keys = client
                        .list_access_keys()
                        .user_name(&cred.principal)
                        .send()
                        .await?
                        .access_key_metadata;

                    if keys.len() >= 2 {
                        // Delete one existing key to free a slot.
                        // (For simplicity, delete the first. In production, pick the least-recently-used or inactive.)
                        if let Some(first) = keys.first() {
                            if let Some(old_id) = first.access_key_id.as_deref() {
                                client
                                    .delete_access_key()
                                    .user_name(&cred.principal)
                                    .access_key_id(old_id)
                                    .send()
                                    .await?;
                            }
                        }
                    }

                    // 2) Create a new key
                    let created = client.create_access_key().user_name(&cred.principal).send().await?;
                    let Some(new_key) = created.access_key() else {
                        bail!("Failed to create access key: new key is None")
                    };
                    let new_id = &new_key.access_key_id;
                    let new_secret = &new_key.secret_access_key;

                    // (Optional but recommended) briefly disable old keys before deleting if you want a canary check.
                    // 3) Retire any remaining old keys
                    let remaining = client
                        .list_access_keys()
                        .user_name(&cred.principal)
                        .send()
                        .await?
                        .access_key_metadata;

                    for md in remaining {
                        let Some(akid) = md.access_key_id() else { continue };
                        if akid != new_id {
                            // You can set to Inactive first if you prefer a two-step cutover:
                            // iam.update_access_key()
                            //     .user_name(USERNAME)
                            //     .access_key_id(akid)
                            //     .status(StatusType::Inactive)
                            //     .send()
                            //     .await?;
                            client
                                .delete_access_key()
                                .user_name(&cred.principal)
                                .access_key_id(akid)
                                .send()
                                .await?;
                        }
                    }

                    // (Optional) ensure the new key is active
                    // client.update_access_key()
                    //     .user_name()
                    //     .access_key_id(&new_id)
                    //     .status(StatusType::Active)
                    //     .send()
                    //     .await?;
                }
            }
        }

        Ok(res)
    }
}
