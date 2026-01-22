use std::{collections::HashSet, path::Path};

use crate::{
    addr::IamResourceAddress,
    resource::IamGroup,
    util::{policies_added, policies_removed},
    util::{users_added, users_removed},
};
use autoschematic_core::{
    connector::{ConnectorOp, PlanResponseElement, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values},
};
use op::IamConnectorOp;
use resource::{IamPolicy, IamRole, IamUser};

use crate::{op, resource};

use super::IamConnector;

impl IamConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;

        let mut res = Vec::new();

        match addr {
            IamResourceAddress::User { path, name } => {
                match (current, desired) {
                    (None, None) => {}
                    (None, Some(new_user)) => {
                        let new_user: IamUser = RON.from_str(&new_user)?;

                        res.push(connector_op!(
                            IamConnectorOp::CreateUser(new_user.clone()),
                            format!("Create new IAM user {}{}", path, name)
                        ));

                        for policy in new_user.attached_policies {
                            res.push(connector_op!(
                                IamConnectorOp::AttachUserPolicy(policy.clone(),),
                                format!("Attach policy `{}` for IAM user `{}{}`", policy, path, name,)
                            ));
                        }
                    }

                    (Some(_old_user), None) => res.push(connector_op!(
                        IamConnectorOp::DeleteUser,
                        format!("DELETE IAM user {}{}", path, name)
                    )),

                    (Some(old_user), Some(new_user)) => {
                        let old_user: IamUser = RON.from_str(&old_user)?;
                        let new_user: IamUser = RON.from_str(&new_user)?;

                        if old_user == new_user {
                            // pass
                        } else {
                            if old_user.tags != new_user.tags {
                                let diff = diff_ron_values(&old_user.tags, &new_user.tags).unwrap_or_default();
                                res.push(connector_op!(
                                    IamConnectorOp::UpdateUserTags(old_user.tags, new_user.tags,),
                                    format!("Modify tags for IAM user `{}{}`\n{}", path, name, diff)
                                ));
                            }

                            for removed_policy in policies_removed(&old_user.attached_policies, &new_user.attached_policies) {
                                res.push(connector_op!(
                                    IamConnectorOp::DetachUserPolicy(removed_policy.clone()),
                                    format!("Detach policy `{}` from IAM user `{}{}`", removed_policy, path, name,)
                                ));
                            }

                            for added_policy in policies_added(&old_user.attached_policies, &new_user.attached_policies) {
                                res.push(connector_op!(
                                    IamConnectorOp::AttachUserPolicy(added_policy.clone(),),
                                    format!("Attach policy `{}` for IAM user `{}{}`", added_policy, path, name,)
                                ));
                            }
                        }
                    }
                }
            }
            IamResourceAddress::Role { path, name } => {
                match (current, desired) {
                    (None, None) => {}
                    (None, Some(new_role)) => {
                        let new_role: IamRole = RON.from_str(&new_role)?;
                        res.push(connector_op!(
                            IamConnectorOp::CreateRole(new_role.clone()),
                            format!("Create new IAM role {}{}", path, name)
                        ));

                        for policy in new_role.attached_policies {
                            res.push(connector_op!(
                                IamConnectorOp::AttachRolePolicy(policy.clone(),),
                                format!("Attach policy `{}` for IAM role `{}{}`", policy, path, name,)
                            ));
                        }
                    }
                    (Some(_old_role), None) => {
                        // let old_role: IamRole = RON.from_str(&old_role).unwrap();
                        res.push(connector_op!(
                            IamConnectorOp::DeleteRole,
                            format!("DELETE IAM role {}{}", path, name)
                        ))
                    }
                    (Some(old_role), Some(new_role)) => {
                        let old_role: IamRole = RON.from_str(&old_role)?;
                        let new_role: IamRole = RON.from_str(&new_role)?;

                        // #plan_cover(assume_role_policy_document)
                        if old_role.assume_role_policy_document != new_role.assume_role_policy_document {
                            let diff =
                                diff_ron_values(&old_role.assume_role_policy_document, &new_role.assume_role_policy_document)
                                    .unwrap_or_default();
                            res.push(connector_op!(
                                IamConnectorOp::UpdateAssumeRolePolicy(
                                    old_role.assume_role_policy_document,
                                    new_role.assume_role_policy_document,
                                ),
                                format!("Modify AssumeRolePolicy for IAM role `{}{}`\n{}", path, name, diff)
                            ));
                        }

                        // #plan_cover(tags)
                        if old_role.tags != new_role.tags {
                            let diff = diff_ron_values(&old_role.tags, &new_role.tags).unwrap_or_default();
                            res.push(connector_op!(
                                IamConnectorOp::UpdateRoleTags(old_role.tags, new_role.tags,),
                                format!("Modify tags for IAM role `{}{}`\n{}", path, name, diff)
                            ));
                        }

                        // #plan_cover(attached_policies)
                        for removed_policy in policies_removed(&old_role.attached_policies, &new_role.attached_policies) {
                            res.push(connector_op!(
                                IamConnectorOp::DetachRolePolicy(removed_policy.clone()),
                                format!("Detach policy `{}` from IAM role `{}{}`", removed_policy, path, name,)
                            ));
                        }

                        for added_policy in policies_added(&old_role.attached_policies, &new_role.attached_policies) {
                            res.push(connector_op!(
                                IamConnectorOp::AttachRolePolicy(added_policy.clone(),),
                                format!("Attach policy `{}` for IAM role `{}{}`", added_policy, path, name,)
                            ));
                        }
                    }
                }
            }
            IamResourceAddress::Group { path, name } => {
                match (current, desired) {
                    (None, None) => {}
                    (None, Some(new_group)) => {
                        let new_group: IamGroup = RON.from_str(&new_group)?;
                        res.push(connector_op!(
                            IamConnectorOp::CreateGroup,
                            format!("Create new IAM Group `{}{}`", path, name)
                        ));

                        for user_name in new_group.users {
                            res.push(connector_op!(
                                IamConnectorOp::AddUserToGroup(user_name.clone()),
                                format!("Add user `{}` to IAM Group `{}{}`", user_name, path, name)
                            ));
                        }

                        for policy in new_group.attached_policies {
                            res.push(connector_op!(
                                IamConnectorOp::AttachGroupPolicy(policy.clone()),
                                format!("Attach policy `{}` to IAM Group `{}{}`", policy, path, name)
                            ));
                        }
                    }
                    (Some(_old_group), None) => {
                        res.push(connector_op!(
                            IamConnectorOp::DeleteGroup,
                            format!("DELETE  IAM Group `{}{}`", path, name)
                        ));
                    }
                    (Some(old_group), Some(new_group)) => {
                        let old_group: IamGroup = RON.from_str(&old_group)?;
                        let new_group: IamGroup = RON.from_str(&new_group)?;

                        if old_group != new_group {
                            for removed_policy in policies_removed(&old_group.attached_policies, &new_group.attached_policies) {
                                res.push(connector_op!(
                                    IamConnectorOp::DetachGroupPolicy(removed_policy.clone()),
                                    format!("Detach policy `{}` from IAM Group `{}{}`", removed_policy, path, name)
                                ));
                            }

                            for added_policy in policies_added(&old_group.attached_policies, &new_group.attached_policies) {
                                res.push(connector_op!(
                                    IamConnectorOp::AttachGroupPolicy(added_policy.clone(),),
                                    format!("Attach policy `{}` to IAM Group `{}{}`", added_policy, path, name)
                                ));
                            }

                            // Handle users
                            for removed_user in users_removed(&old_group.users, &new_group.users) {
                                res.push(connector_op!(
                                    IamConnectorOp::RemoveUserFromGroup(removed_user.clone()),
                                    format!("Remove user `{}` from IAM Group `{}{}`", removed_user, path, name)
                                ));
                            }

                            for added_user in users_added(&old_group.users, &new_group.users) {
                                res.push(connector_op!(
                                    IamConnectorOp::AddUserToGroup(added_user.clone(),),
                                    format!("Add user `{}` to IAM Group `{}{}`", added_user, path, name)
                                ));
                            }
                            // If IamGroup had tags, this is where they would be handled.
                        }
                    }
                }
            }
            IamResourceAddress::Policy { name, .. } => match (current, desired) {
                (None, None) => {}
                (None, Some(new_policy)) => {
                    let new_policy: IamPolicy = RON.from_str(&new_policy)?;
                    res.push(connector_op!(
                        IamConnectorOp::CreatePolicy(new_policy),
                        format!("Create new IAM policy {}", name)
                    ));
                }
                (Some(_old_policy), None) => res.push(connector_op!(
                    IamConnectorOp::DeletePolicy,
                    format!("DELETE IAM policy {}", name)
                )),
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: IamPolicy = RON.from_str(&old_policy)?;
                    let new_policy: IamPolicy = RON.from_str(&new_policy)?;

                    if old_policy.policy_document != new_policy.policy_document {
                        let diff =
                            diff_ron_values(&old_policy.policy_document, &new_policy.policy_document).unwrap_or_default();
                        res.push(connector_op!(
                            IamConnectorOp::UpdatePolicyDocument(old_policy.policy_document, new_policy.policy_document,),
                            format!("Modify policy document for IAM policy `{}`\n{}", name, diff)
                        ));
                    }

                    if old_policy.tags != new_policy.tags {
                        let diff = diff_ron_values(&old_policy.tags, &new_policy.tags).unwrap_or_default();
                        res.push(connector_op!(
                            IamConnectorOp::UpdatePolicyTags(old_policy.tags, new_policy.tags,),
                            format!("Modify tags for IAM policy `{}`\n{}", name, diff)
                        ));
                    }
                }
            },
        }

        Ok(res)
    }
}
