use std::{
    collections::HashMap, ffi::OsString, path::{Path, PathBuf}, time::Duration
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
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = IamResourceAddress::from_path(addr)?;
        match addr {
            IamResourceAddress::User(user_name) => match (current, desired) {
                (None, None) => Ok(vec![]),
                (None, Some(new_user)) => {
                    let new_user: IamUser = RON.from_str(&new_user)?;
                    Ok(vec![connector_op!(
                        IamConnectorOp::CreateUser(new_user),
                        format!("Create new IAM user {}", user_name)
                    )])
                }
                (Some(_old_user), None) => {
                    // let old_user: IamUser = RON.from_str(&old_user).unwrap();
                    Ok(vec![connector_op!(
                        IamConnectorOp::DeleteUser,
                        format!("DELETE IAM user {}", user_name)
                    )])
                }
                (Some(old_user), Some(new_user)) => {
                    let old_user: IamUser = RON.from_str(&old_user)?;
                    let new_user: IamUser = RON.from_str(&new_user)?;
                    let mut ops = Vec::new();
                    if old_user == new_user {
                        Ok(Vec::new())
                    } else {
                        if old_user.tags != new_user.tags {
                            let diff =
                                diff_ron_values(&old_user.tags, &new_user.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                IamConnectorOp::UpdateUserTags(old_user.tags, new_user.tags,),
                                format!("Modify tags for IAM user `{}`\n{}", user_name, diff)
                            ));
                        }

                        if old_user.attached_policies != new_user.attached_policies {
                            for new_policy in &new_user.attached_policies {
                                if !old_user.attached_policies.contains(new_policy) {
                                    ops.push(connector_op!(
                                        IamConnectorOp::AttachUserPolicy(new_policy.clone(),),
                                        format!(
                                            "Attach policy `{}` for IAM user `{}`",
                                            new_policy, user_name,
                                        )
                                    ));
                                }
                            }
                            for old_policy in &old_user.attached_policies {
                                if !new_user.attached_policies.contains(old_policy) {
                                    ops.push(connector_op!(
                                        IamConnectorOp::DetachUserPolicy(old_policy.clone()),
                                        format!(
                                            "Detach policy `{}` from IAM user `{}`",
                                            old_policy, user_name,
                                        )
                                    ));
                                }
                            }
                        }

                        Ok(ops)
                    }
                }
            },
            IamResourceAddress::Role(role_name) => match (current, desired) {
                (None, None) => Ok(vec![]),
                (None, Some(new_role)) => {
                    let new_role: IamRole = RON.from_str(&new_role)?;
                    Ok(vec![connector_op!(
                        IamConnectorOp::CreateRole(new_role),
                        format!("Create new IAM role {}", role_name)
                    )])
                }
                (Some(_old_role), None) => {
                    // let old_role: IamRole = RON.from_str(&old_role).unwrap();
                    Ok(vec![connector_op!(
                        IamConnectorOp::DeleteRole,
                        format!("DELETE IAM role {}", role_name)
                    )])
                }
                (Some(old_role), Some(new_role)) => {
                    let old_role: IamRole = RON.from_str(&old_role)?;
                    let new_role: IamRole = RON.from_str(&new_role)?;
                    let mut ops = Vec::new();

                    if old_role.assume_role_policy_document != new_role.assume_role_policy_document
                    {
                        let diff = diff_ron_values(
                            &old_role.assume_role_policy_document,
                            &new_role.assume_role_policy_document,
                        )
                        .unwrap_or_default();
                        ops.push(connector_op!(
                            IamConnectorOp::UpdateAssumeRolePolicy(
                                old_role.assume_role_policy_document,
                                new_role.assume_role_policy_document,
                            ),
                            format!(
                                "Modify AssumeRolePolicy for IAM role `{}`\n{}",
                                role_name, diff
                            )
                        ));
                    }

                    if old_role.tags != new_role.tags {
                        let diff =
                            diff_ron_values(&old_role.tags, &new_role.tags).unwrap_or_default();
                        ops.push(connector_op!(
                            IamConnectorOp::UpdateRoleTags(old_role.tags, new_role.tags,),
                            format!("Modify tags for IAM role `{}`\n{}", role_name, diff)
                        ));
                    }

                    Ok(ops)
                }
            },
            IamResourceAddress::Policy(policy_name) => match (current, desired) {
                (None, None) => Ok(vec![]),
                (None, Some(new_policy)) => {
                    let new_policy: IamPolicy = RON.from_str(&new_policy)?;
                    Ok(vec![connector_op!(
                        IamConnectorOp::CreatePolicy(new_policy),
                        format!("Create new IAM policy {}", policy_name)
                    )])
                }
                (Some(_old_policy), None) => {
                    // let old_policy: IamPolicy = RON.from_str(&old_policy).unwrap();
                    Ok(vec![connector_op!(
                        IamConnectorOp::DeletePolicy,
                        format!("DELETE IAM policy {}", policy_name)
                    )])
                }
                (Some(old_policy), Some(new_policy)) => {
                    let old_policy: IamPolicy = RON.from_str(&old_policy)?;
                    let new_policy: IamPolicy = RON.from_str(&new_policy)?;
                    let mut ops = Vec::new();

                    if old_policy.policy_document != new_policy.policy_document {
                        let diff = diff_ron_values(
                            &old_policy.policy_document,
                            &new_policy.policy_document,
                        )
                        .unwrap_or_default();
                        ops.push(connector_op!(
                            IamConnectorOp::UpdatePolicyDocument(
                                old_policy.policy_document,
                                new_policy.policy_document,
                            ),
                            format!(
                                "Modify policy document for IAM policy `{}`\n{}",
                                policy_name, diff
                            )
                        ));
                    }

                    if old_policy.tags != new_policy.tags {
                        let diff =
                            diff_ron_values(&old_policy.tags, &new_policy.tags).unwrap_or_default();
                        ops.push(connector_op!(
                            IamConnectorOp::UpdatePolicyTags(old_policy.tags, new_policy.tags,),
                            format!("Modify tags for IAM policy `{}`\n{}", policy_name, diff)
                        ));
                    }

                    Ok(ops)
                }
            },
            _ => Ok(vec![]),
        }
    }
}
