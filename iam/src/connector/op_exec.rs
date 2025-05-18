use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::addr::IamResourceAddress;
use anyhow::{bail, Context};
use async_trait::async_trait;
use autoschematic_connector_aws_core::config::AwsConnectorConfig;
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

use super::IamConnector;

impl IamConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;
        let op = IamConnectorOp::from_str(op)?;

        match &addr {
            Some(IamResourceAddress::User(user_name)) => match op {
                IamConnectorOp::CreateUser(user) => {
                    self.client
                        .create_user()
                        .user_name(user_name)
                        .set_tags(user.tags.into())
                        .send()
                        .await?;
                    let arn = format!("arn:aws:iam::{}:user/{}", self.account_id, user_name);
                    op_exec_output!(
                        Some([("user_arn", Some(arn))]),
                        format!("Created IAM user `{}`", user_name)
                    )
                }
                IamConnectorOp::DeleteUser => {
                    self.client
                        .delete_user()
                        .user_name(user_name)
                        .send()
                        .await?;

                    op_exec_output!(
                        Some([("user_arn", Option::<String>::None)]),
                        format!("Deleted IAM user `{}`", user_name)
                    )
                }
                IamConnectorOp::AttachUserPolicy(policy) => {
                    let policy_arn = format!("arn:aws:iam::aws:policy/{}", policy);
                    self.client
                        .attach_user_policy()
                        .policy_arn(policy_arn)
                        .user_name(user_name)
                        .send()
                        .await?;
                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Attached policy {} for IAM user `{}`",
                            policy, user_name
                        )),
                    })
                }
                IamConnectorOp::DetachUserPolicy(policy) => {
                    let policy_arn = format!("arn:aws:iam::aws:policy/{}", policy);
                    self.client
                        .detach_user_policy()
                        .policy_arn(policy_arn)
                        .user_name(user_name)
                        .send()
                        .await?;
                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Detached policy {} for IAM user `{}`",
                            policy, user_name
                        )),
                    })
                }
                IamConnectorOp::UpdateUserTags(old_tags, new_tags) => {
                    let (untag_keys, new_tagset) =
                        tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                    if untag_keys.len() > 0 {
                        self.client
                            .untag_user()
                            .user_name(user_name)
                            .set_tag_keys(Some(untag_keys))
                            .send()
                            .await
                            .context("Failed to remove tags")?;
                    }

                    if new_tagset.len() > 0 {
                        self.client
                            .tag_user()
                            .user_name(user_name)
                            .set_tags(Some(new_tagset))
                            .send()
                            .await
                            .context("Failed to write new tags")?;
                    }

                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!("Updated tags for IAM role {}", &user_name)),
                    })
                }
                _ => bail!(
                    "Invalid Op {:#?} for addr {:?}! This is a bug in the connector.",
                    op,
                    addr
                ),
            },
            Some(IamResourceAddress::Role(role_name)) => match op {
                IamConnectorOp::CreateRole(role) => {
                    if let Some(assume_role_policy) = role.assume_role_policy_document {
                        self.client
                            .create_role()
                            .role_name(role_name)
                            .assume_role_policy_document(serde_json::to_string(
                                &assume_role_policy,
                            )?)
                            .set_tags(role.tags.into())
                            .send()
                            .await?;
                    } else {
                        self.client
                            .create_role()
                            .role_name(role_name)
                            .send()
                            .await?;
                    }

                    let arn = format!("arn:aws:iam::{}:role/{}", self.account_id, role_name);
                    op_exec_output!(
                        Some([("role_arn", Some(arn))]),
                        format!("Created IAM role `{}`", &role_name)
                    )
                }
                IamConnectorOp::DeleteRole => {
                    self.client
                        .delete_role()
                        .role_name(role_name)
                        .send()
                        .await?;
                    op_exec_output!(
                        Some([("role_arn", Option::<String>::None)]),
                        format!("Deleted IAM role `{}`", role_name)
                    )
                }
                IamConnectorOp::UpdateAssumeRolePolicy(_old_policy, new_policy) => {
                    // self.client.update_assume_role_policy()
                    let policy_json = match new_policy {
                        Some(new_policy) => serde_json::to_string(&new_policy)
                            .context("Failed to serialize AssumeRolePolicy as JSON")?,
                        None => String::new(),
                    };
                    self.client
                        .update_assume_role_policy()
                        .role_name(role_name)
                        .policy_document(policy_json)
                        .send()
                        .await
                        .context("Failed to update AssumeRolePolicy!")?;

                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Updated AssumRolePolicy for IAM role {}",
                            &role_name
                        )),
                    })
                }
                IamConnectorOp::UpdateRoleTags(old_tags, new_tags) => {
                    let (untag_keys, new_tagset) =
                        tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                    if untag_keys.len() > 0 {
                        self.client
                            .untag_role()
                            .role_name(role_name)
                            .set_tag_keys(Some(untag_keys))
                            .send()
                            .await
                            .context("Failed to remove tags")?;
                    }

                    if new_tagset.len() > 0 {
                        self.client
                            .tag_role()
                            .role_name(role_name)
                            .set_tags(Some(new_tagset))
                            .send()
                            .await
                            .context("Failed to write new tags")?;
                    }

                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Updated tags for IAM role `{}`",
                            &role_name
                        )),
                    })
                }
                _ => bail!(
                    "Invalid Op {:#?} for addr {:?}! This is a bug in the connector.",
                    op,
                    addr
                ),
            },
            Some(IamResourceAddress::Policy(policy_name)) => match op {
                IamConnectorOp::CreatePolicy(policy) => {
                    let policy_document = policy.policy_document;

                    let policy_json = serde_json::to_string(&policy_document)
                        .context("Failed to serialize policy document as JSON")?;

                    self.client
                        .create_policy()
                        .policy_name(policy_name)
                        .policy_document(policy_json)
                        .set_tags(policy.tags.into())
                        .send()
                        .await?;

                    let arn = format!("arn:aws:iam::{}:policy/{}", self.account_id, policy_name);

                    op_exec_output!(
                        Some([("policy_arn", Some(arn))]),
                        format!("Created IAM policy `{}`", &policy_name)
                    )
                }
                IamConnectorOp::DeletePolicy => {
                    let arn = format!("arn:aws:iam::{}:policy/{}", self.account_id, policy_name);
                    self.client.delete_policy().policy_arn(arn).send().await?;
                    op_exec_output!(
                        Some([("policy_arn", Option::<String>::None)]),
                        format!("Deleted IAM policy `{}`", policy_name)
                    )
                }
                IamConnectorOp::UpdatePolicyDocument(_old_policy_document, new_policy_document) => {
                    let policy_arn =
                        format!("arn:aws:iam::{}:policy/{}", self.account_id, policy_name);

                    let policy_json = serde_json::to_string(&new_policy_document)
                        .context("Failed to serialize policy document as JSON")?;

                    let create_policy_version_output = self
                        .client
                        .create_policy_version()
                        .policy_arn(&policy_arn)
                        .policy_document(policy_json)
                        .send()
                        .await
                        .context("Failed to create new policy version")?;

                    let Some(policy_version) = create_policy_version_output.policy_version else {
                        bail!("Failed to create new policy version");
                    };

                    let Some(version_id) = policy_version.version_id else {
                        bail!("Failed to create new policy version");
                    };

                    self.client
                        .set_default_policy_version()
                        .policy_arn(policy_arn)
                        .version_id(version_id)
                        .send()
                        .await
                        .context("Failed to set new default policy version ID")?;

                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Updated policy document for IAM policy `{}`",
                            &policy_name
                        )),
                    })
                }
                IamConnectorOp::UpdatePolicyTags(old_tags, new_tags) => {
                    let policy_arn =
                        format!("arn:aws:iam::{}:policy/{}", self.account_id, policy_name);
                    let (untag_keys, new_tagset) =
                        tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                    if untag_keys.len() > 0 {
                        self.client
                            .untag_policy()
                            .policy_arn(&policy_arn)
                            .set_tag_keys(Some(untag_keys))
                            .send()
                            .await
                            .context("Failed to remove tags")?;
                    }

                    if new_tagset.len() > 0 {
                        self.client
                            .tag_policy()
                            .policy_arn(&policy_arn)
                            .set_tags(Some(new_tagset))
                            .send()
                            .await
                            .context("Failed to write new tags")?;
                    }

                    Ok(OpExecOutput {
                        outputs: None,
                        friendly_message: Some(format!(
                            "Updated tags for IAM policy `{}`",
                            &policy_name
                        )),
                    })
                }
                _ => bail!(
                    "Invalid Op {:#?} for addr {:?}! This is a bug in the connector.",
                    op,
                    addr
                ),
            },
            _ => bail!(
                "Invalid Op {:#?} for addr {:?}! This is a bug in the connector.",
                op,
                addr
            ),
        }
    }
}
