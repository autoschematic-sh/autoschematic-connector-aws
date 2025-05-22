use std::{
    collections::HashMap,
    path::Path,
};

use crate::addr::IamResourceAddress;
use anyhow::{bail, Context};
use autoschematic_core::{
    connector::{
        ConnectorOp, GetResourceOutput,
        Resource, ResourceAddress,
    }, get_resource_output, util::RON
};
use resource::{IamPolicy, IamResource, IamRole, IamUser};

use util::{list_attached_role_policies, list_attached_user_policies};

use crate::{
    resource,
    util::{self},
};

use super::IamConnector;

impl IamConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;
        match addr {
            IamResourceAddress::User(user_name) => {
                let user_result = self.client.get_user().user_name(&user_name).send().await;

                match user_result {
                    Ok(user_output) => {
                        let Some(user) = user_output.user else {
                            return Ok(None);
                        };

                        let attached_policies =
                            list_attached_user_policies(&self.client, &user_name).await?;

                        let iam_user = IamUser {
                            attached_policies,
                            tags: user.tags.into(),
                        };

                        get_resource_output!(
                            IamResource::User(iam_user),
                            [(
                                "user_arn",
                                Some(format!(
                                    "arn:aws:iam::{}:user/{}",
                                    self.account_id, user_name
                                ))
                            )]
                        )
                    }
                    Err(_) => Ok(None),
                }
            }
            IamResourceAddress::Role(role_name) => {
                let role_result = self.client.get_role().role_name(&role_name).send().await;

                match role_result {
                    Ok(role_output) => {
                        let Some(role) = role_output.role else {
                            return Ok(None);
                        };

                        let attached_policies =
                            list_attached_role_policies(&self.client, &role_name).await?;

                        let iam_role =
                            if let Some(assume_role_policy) = role.assume_role_policy_document {
                                let json_s = urlencoding::decode(&assume_role_policy)?;
                                let val: serde_json::Value = serde_json::from_str(&json_s)?;

                                let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;

                                IamRole {
                                    attached_policies,
                                    assume_role_policy_document: Some(rval),
                                    tags: role.tags.into(),
                                }
                            } else {
                                IamRole {
                                    attached_policies,
                                    assume_role_policy_document: None,
                                    tags: role.tags.into(),
                                }
                            };

                        get_resource_output!(
                            IamResource::Role(iam_role),
                            vec![(
                                "role_arn",
                                Some(format!(
                                    "arn:aws:iam::{}:role/{}",
                                    self.account_id, role_name
                                ))
                            )]
                        )
                    }
                    Err(_) => Ok(None),
                }
            }
            IamResourceAddress::Policy(policy_name) => {
                let arn = format!("arn:aws:iam::{}:policy/{}", self.account_id, policy_name);
                let policy_result = self.client.get_policy().policy_arn(&arn).send().await;

                match policy_result {
                    Ok(policy_output) => {
                        let Some(policy) = policy_output.policy else {
                            bail!("Couldn't get policy for ARN {}", arn);
                        };

                        let Some(version_id) = policy.default_version_id else {
                            bail!("Couldn't get default_version_id for ARN {}", arn);
                        };

                        let get_policy_version_output = self
                            .client
                            .get_policy_version()
                            .policy_arn(&arn)
                            .version_id(&version_id)
                            .send()
                            .await;
                        if let Err(e) = get_policy_version_output {
                            bail!(
                                "Couldn't get policy version {} for ARN {}: {}",
                                version_id,
                                arn,
                                e
                            );
                        };

                        let Some(policy_version) =
                            get_policy_version_output.unwrap().policy_version
                        else {
                            bail!("Couldn't get default_version_id for ARN {}", arn);
                        };

                        let Some(document) = policy_version.document else {
                            bail!("Couldn't get document for ARN {}", arn);
                        };

                        let json_s = urlencoding::decode(&document)?;
                        let val: serde_json::Value = serde_json::from_str(&json_s)?;

                        let rval: ron::Value = RON.from_str(&RON.to_string(&val)?)?;

                        let iam_policy = IamPolicy {
                            policy_document: rval,
                            tags: policy.tags.into(),
                        };
                        get_resource_output!(
                            IamResource::Policy(iam_policy),
                            [(
                                "policy_arn",
                                Some(format!(
                                    "arn:aws:iam::{}:policy/{}",
                                    self.account_id, policy_name
                                ))
                            )]
                        )
                    }
                    Err(_) => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }
}