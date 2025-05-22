use std::path::{Path, PathBuf};

use crate::addr::IamResourceAddress;
use autoschematic_core::connector::{
        Connector, ResourceAddress,
    };

use aws_sdk_iam::types::PolicyScopeType;


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