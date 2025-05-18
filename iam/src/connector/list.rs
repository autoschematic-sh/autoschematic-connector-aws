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
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        // List Users
        let users = self.client.list_users().send().await?;
        for user in users.users {
            results.push(IamResourceAddress::User(user.user_name.clone()).to_path_buf());
        }

        // List Roles
        let roles = self.client.list_roles().send().await?;
        for role in roles.roles {
            results.push(IamResourceAddress::Role(role.role_name.clone()).to_path_buf());
        }

        // List Policies (This might need pagination)
        let policies = self
            .client
            .list_policies()
            .scope(PolicyScopeType::Local)
            .send()
            .await?;
        if let Some(policies) = policies.policies {
            for policy in policies {
                if let Some(name) = policy.policy_name {
                    results.push(IamResourceAddress::Policy(name).to_path_buf());
                }
            }
        }

        Ok(results)
    }
}