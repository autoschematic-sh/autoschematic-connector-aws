use std::path::Path;

use crate::{addr::IamResourceAddress, resource::IamGroup, util::list_attached_group_policies};
use anyhow::{Context, bail};
use autoschematic_core::{
    connector::{GetResourceResponse, Resource, ResourceAddress},
    get_resource_response,
    util::RON,
};
use resource::{IamPolicy, IamResource, IamRole, IamUser};

use util::{list_attached_role_policies, list_attached_user_policies};

use crate::{
    resource,
    util::{self},
};

use super::IamConnector;

impl IamConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;
        let Some(client) = self.client.read().await.clone() else {
            bail!("No client");
        };
        let Some(account_id) = self.account_id.read().await.clone() else {
            bail!("No account ID");
        };

        match addr {
            IamResourceAddress::User { path, name } => {
                let user_result = client.get_user().user_name(&name).send().await;

                match user_result {
                    Ok(user_output) => {
                        let Some(user) = user_output.user else {
                            return Ok(None);
                        };

                        let attached_policies = list_attached_user_policies(&client, &name).await?;

                        let iam_user = IamUser {
                            attached_policies,
                            tags: user.tags.into(),
                        };

                        get_resource_response!(IamResource::User(iam_user))
                    }
                    Err(e) => {
                        match e.as_service_error() {
                            Some(aws_sdk_iam::operation::get_user::GetUserError::NoSuchEntityException(_)) => Ok(None),
                            _ => Err(e.into()),
                        }
                    }
                }
            }

            IamResourceAddress::Role { path, name } => {
                let role_result = client.get_role().role_name(&name).send().await;

                match role_result {
                    Ok(role_output) => {
                        let Some(role) = role_output.role else {
                            return Ok(None);
                        };

                        let attached_policies = list_attached_role_policies(&client, &name).await?;

                        let iam_role = if let Some(assume_role_policy) = role.assume_role_policy_document {
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

                        get_resource_response!(IamResource::Role(iam_role))
                    }
                    Err(e) => {
                        match e.as_service_error() {
                            Some(aws_sdk_iam::operation::get_role::GetRoleError::NoSuchEntityException(_)) => Ok(None),
                            _ => Err(e.into()),
                        }
                    }
                }
            }

            IamResourceAddress::Group { path, name } => {
                let group_result = client.get_group().group_name(&name).send().await;

                match group_result {
                    Ok(group_output) => {
                        let Some(ref group) = group_output.group else {
                            return Ok(None);
                        };

                        let group_user_names = group_output.users().iter().map(|user| user.user_name.clone()).collect();

                        let attached_policies = list_attached_group_policies(&client, &name).await?;

                        let iam_group = IamGroup {
                            users: group_user_names,
                            attached_policies,
                        };

                        get_resource_response!(IamResource::Group(iam_group))
                    }
                    Err(e) => {
                        match e.as_service_error() {
                            Some(aws_sdk_iam::operation::get_group::GetGroupError::NoSuchEntityException(_)) => Ok(None),
                            _ => Err(e.into()),
                        }
                    }
                }
            }
            IamResourceAddress::Policy { path, name } => {
                let arn = format!("arn:aws:iam::{account_id}:policy{path}{name}");
                let policy_result = client.get_policy().policy_arn(&arn).send().await;

                match policy_result {
                    Ok(policy_output) => {
                        let Some(policy) = policy_output.policy else {
                            bail!("Couldn't get policy for ARN {}", arn);
                        };

                        let Some(version_id) = policy.default_version_id else {
                            bail!("Couldn't get default_version_id for ARN {}", arn);
                        };

                        let get_policy_version_output = client
                            .get_policy_version()
                            .policy_arn(&arn)
                            .version_id(&version_id)
                            .send()
                            .await;

                        if let Err(e) = get_policy_version_output {
                            bail!("Couldn't get policy version {} for ARN {}: {}", version_id, arn, e);
                        };

                        let Some(policy_version) = get_policy_version_output.unwrap().policy_version else {
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

                        get_resource_response!(IamResource::Policy(iam_policy))
                    }
                    Err(e) => {
                        match e.as_service_error() {
                            Some(aws_sdk_iam::operation::get_policy::GetPolicyError::NoSuchEntityException(_)) => Ok(None),
                            _ => Err(e.into()),
                        }
                    }
                }
            }
        }
    }
}
