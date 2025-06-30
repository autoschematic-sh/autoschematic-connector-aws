use std::{collections::HashMap, path::Path};

use crate::addr::IamResourceAddress;
use anyhow::{Context, bail};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, OutputMapExec, OutputValueExec, ResourceAddress},
    error_util::invalid_op,
    op_exec_output,
};

use op::IamConnectorOp;

use tags::tag_diff;

use crate::{op, tags};

use super::IamConnector;

impl IamConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;
        let op = IamConnectorOp::from_str(op)?;
        let Some(ref client) = *self.client.read().await else {
            bail!("No client")
        };
        let Some(account_id) = self.account_id.read().await.clone() else {
            bail!("No account ID")
        };

        match &addr {
            IamResourceAddress::User { path, name } => {
                match op {
                    IamConnectorOp::CreateUser(user) => {
                        client.create_user().user_name(name).set_tags(user.tags.into()).send().await?;
                        let arn = format!("arn:aws:iam::{}:user/{}", account_id, name);
                        op_exec_output!(Some([("arn", Some(arn))]), format!("Created IAM user `{}`", name))
                    }
                    IamConnectorOp::DeleteUser => {
                        client.delete_user().user_name(name).send().await?;

                        op_exec_output!(
                            format!("Deleted IAM user `{}{}`", path, name)
                        )
                    }
                    IamConnectorOp::AttachUserPolicy(policy) => {
                        let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);
                        client
                            .attach_user_policy()
                            .policy_arn(policy_arn)
                            .user_name(name)
                            .send()
                            .await?;
                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Attached policy {} for IAM user `{}`", policy, name)),
                        })
                    }
                    IamConnectorOp::DetachUserPolicy(policy) => {
                        let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);

                        client
                            .detach_user_policy()
                            .policy_arn(policy_arn)
                            .user_name(name)
                            .send()
                            .await?;
                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Detached policy {} for IAM user `{}`", policy, name)),
                        })
                    }
                    IamConnectorOp::UpdateUserTags(old_tags, new_tags) => {
                        let (untag_keys, new_tagset) = tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                        if !untag_keys.is_empty() {
                            client
                                .untag_user()
                                .user_name(name)
                                .set_tag_keys(Some(untag_keys))
                                .send()
                                .await
                                .context("Failed to remove tags")?;
                        }

                        if !new_tagset.is_empty() {
                            client
                                .tag_user()
                                .user_name(name)
                                .set_tags(Some(new_tagset))
                                .send()
                                .await
                                .context("Failed to write new tags")?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for IAM role {}", &name)),
                        })
                    }
                    _ => bail!("Invalid Op {:#?} for addr {:?}! This is a bug in the connector.", op, addr),
                }
            }
            IamResourceAddress::Role { path, name } => {
                match op {
                    IamConnectorOp::CreateRole(role) => {
                        if let Some(assume_role_policy) = role.assume_role_policy_document {
                            client
                                .create_role()
                                .role_name(name)
                                .assume_role_policy_document(serde_json::to_string(&assume_role_policy)?)
                                .set_tags(role.tags.into())
                                .send()
                                .await?;
                        } else {
                            client.create_role().role_name(name).send().await?;
                        }

                        let arn = format!("arn:aws:iam::{}:role/{}", account_id, name);
                        op_exec_output!(Some([("arn", Some(arn))]), format!("Created IAM role `{}{}`", path, &name))
                    }
                    IamConnectorOp::DeleteRole => {
                        client.delete_role().role_name(name).send().await?;
                        op_exec_output!(
                            format!("Deleted IAM role `{}`", name)
                        )
                    }
                    IamConnectorOp::AttachRolePolicy(policy_arn) => {
                        // let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);

                        client
                            .attach_role_policy()
                            .role_name(name)
                            .policy_arn(&policy_arn)
                            .send()
                            .await?;

                        op_exec_output!(format!("Attached policy `{}` to role `{}{}`", &policy_arn, path, &name))
                    }
                    IamConnectorOp::DetachRolePolicy(policy_arn) => {
                        // let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);

                        client
                            .detach_role_policy()
                            .role_name(name)
                            .policy_arn(&policy_arn)
                            .send()
                            .await?;

                        op_exec_output!(format!("Detached policy `{}` from role `{}{}`", &policy_arn, path, &name))
                    }
                    IamConnectorOp::UpdateAssumeRolePolicy(_old_policy, new_policy) => {
                        // self.client.update_assume_role_policy()
                        let policy_json = match new_policy {
                            Some(new_policy) => {
                                serde_json::to_string(&new_policy).context("Failed to serialize AssumeRolePolicy as JSON")?
                            }
                            None => String::new(),
                        };
                        client
                            .update_assume_role_policy()
                            .role_name(name)
                            .policy_document(policy_json)
                            .send()
                            .await
                            .context("Failed to update AssumeRolePolicy!")?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated AssumRolePolicy for IAM role `{}{}`", path, &name)),
                        })
                    }
                    IamConnectorOp::UpdateRoleTags(old_tags, new_tags) => {
                        let (untag_keys, new_tagset) = tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                        if !untag_keys.is_empty() {
                            client
                                .untag_role()
                                .role_name(name)
                                .set_tag_keys(Some(untag_keys))
                                .send()
                                .await
                                .context("Failed to remove tags")?;
                        }

                        if !new_tagset.is_empty() {
                            client
                                .tag_role()
                                .role_name(name)
                                .set_tags(Some(new_tagset))
                                .send()
                                .await
                                .context("Failed to write new tags")?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for IAM role `{}{}`", path, &name)),
                        })
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            IamResourceAddress::Group { path, name } => {
                match op {
                    IamConnectorOp::CreateGroup => {
                        client.create_group().group_name(name).path(path).send().await?;
                        op_exec_output!(format!("Created group `{}{}`", path, &name))
                    }
                    IamConnectorOp::AddUserToGroup(user_name) => {
                        client
                            .add_user_to_group()
                            .group_name(name)
                            .user_name(&user_name)
                            .send()
                            .await?;
                        op_exec_output!(format!("Added user `{}` to group `{}{}`", &user_name, path, &name))
                    }
                    IamConnectorOp::AttachGroupPolicy(policy_arn) => {
                        // let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);

                        client
                            .attach_group_policy()
                            .group_name(name)
                            .policy_arn(&policy_arn)
                            .send()
                            .await?;

                        op_exec_output!(format!("Attached policy `{}` to group `{}{}`", &policy_arn, path, &name))
                    }
                    IamConnectorOp::DetachGroupPolicy(policy_arn) => {
                        // let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, policy);

                        client
                            .detach_group_policy()
                            .group_name(name)
                            .policy_arn(&policy_arn)
                            .send()
                            .await?;

                        op_exec_output!(format!("Detached policy `{}` from group `{}{}`", &policy_arn, path, &name))
                    }
                    IamConnectorOp::RemoveUserFromGroup(user_name) => {
                        client
                            .remove_user_from_group()
                            .group_name(name)
                            .user_name(&user_name)
                            .send()
                            .await?;
                        op_exec_output!(format!("Removed user `{}` from group `{}{}`", &user_name, path, &name))
                    }
                    IamConnectorOp::DeleteGroup => {
                        client.delete_group().group_name(name).send().await?;
                        op_exec_output!(format!("Deleted IAM group `{}`", &name))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            IamResourceAddress::Policy { path, name } => {
                match op {
                    IamConnectorOp::CreatePolicy(policy) => {
                        let policy_document = policy.policy_document;

                        let policy_json =
                            serde_json::to_string(&policy_document).context("Failed to serialize policy document as JSON")?;

                        client
                            .create_policy()
                            .policy_name(name)
                            .path(path)
                            .policy_document(policy_json)
                            .set_tags(policy.tags.into())
                            .send()
                            .await?;

                        let policy_arn = format!("arn:aws:iam::{}:policy/{}", account_id, name);

                        op_exec_output!(
                            format!("Created IAM policy `{}{}`", path, &name)
                        )
                    }
                    IamConnectorOp::DeletePolicy => {
                        let policy_arn = format!("arn:aws:iam::{}:policy/{}{}", account_id, path, name);

                        client.delete_policy().policy_arn(policy_arn).send().await?;
                        op_exec_output!(
                            format!("Deleted IAM policy `{}`", name)
                        )
                    }
                    IamConnectorOp::UpdatePolicyDocument(_old_policy_document, new_policy_document) => {
                        let policy_arn = format!("arn:aws:iam::{}:policy/{}{}", account_id, path, name);

                        let policy_json = serde_json::to_string(&new_policy_document)
                            .context("Failed to serialize policy document as JSON")?;

                        let create_policy_version_output = client
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

                        client
                            .set_default_policy_version()
                            .policy_arn(policy_arn)
                            .version_id(version_id)
                            .send()
                            .await
                            .context("Failed to set new default policy version ID")?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated policy document for IAM policy `{}`", &name)),
                        })
                    }
                    IamConnectorOp::UpdatePolicyTags(old_tags, new_tags) => {
                        let policy_arn = format!("arn:aws:iam::{}:policy/{}{}", account_id, path, name);
                        let (untag_keys, new_tagset) = tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                        if !untag_keys.is_empty() {
                            client
                                .untag_policy()
                                .policy_arn(&policy_arn)
                                .set_tag_keys(Some(untag_keys))
                                .send()
                                .await
                                .context("Failed to remove tags")?;
                        }

                        if !new_tagset.is_empty() {
                            client
                                .tag_policy()
                                .policy_arn(&policy_arn)
                                .set_tags(Some(new_tagset))
                                .send()
                                .await
                                .context("Failed to write new tags")?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for IAM policy `{}`", &name)),
                        })
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
