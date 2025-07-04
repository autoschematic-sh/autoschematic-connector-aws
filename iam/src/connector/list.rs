use std::path::{Path, PathBuf};

use crate::addr::IamResourceAddress;
use anyhow::bail;
use autoschematic_connector_aws_core::arn::parse_arn;
use autoschematic_core::{connector::ResourceAddress, glob::addr_matches_filter};

use super::IamConnector;

impl IamConnector {
    pub async fn do_list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let Some(ref account_id) = *self.account_id.read().await else {
            bail!("Account ID not set")
        };

        let mut results = Vec::<PathBuf>::new();
        let Some(ref client) = *self.client.read().await else {
            bail!("No client")
        };

        if addr_matches_filter(&PathBuf::from("aws/iam/users"), subpath) {
            let mut users = client.list_users().into_paginator().send();

            while let Some(users) = users.next().await {
                for user in users?.users {
                    if parse_arn(&user.arn)?.account_id == account_id {
                        results.push(
                            IamResourceAddress::User {
                                path: user.path,
                                name: user.user_name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        if addr_matches_filter(&PathBuf::from("aws/iam/roles"), subpath) {
            let mut roles = client.list_roles().into_paginator().send();

            while let Some(roles) = roles.next().await {
                for role in roles?.roles {
                    if parse_arn(&role.arn)?.account_id == account_id {
                        results.push(
                            IamResourceAddress::Role {
                                path: role.path,
                                name: role.role_name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        if addr_matches_filter(&PathBuf::from("aws/iam/roles"), subpath) {
            let mut groups = client.list_groups().into_paginator().send();

            while let Some(groups) = groups.next().await {
                for group in groups?.groups {
                    if parse_arn(&group.arn)?.account_id == account_id {
                        results.push(
                            IamResourceAddress::Group {
                                path: group.path,
                                name: group.group_name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        if addr_matches_filter(&PathBuf::from("aws/iam/policies"), subpath) {
            let mut policies = client.list_policies().into_paginator().send();

            while let Some(policies) = policies.next().await {
                if let Some(policies) = policies?.policies {
                    for policy in policies {
                        if let (Some(path), Some(name), Some(arn)) = (policy.path, policy.policy_name, policy.arn)
                            && parse_arn(&arn)?.account_id == account_id {
                                results.push(IamResourceAddress::Policy { path, name }.to_path_buf());
                            }
                    }
                }
            }
        }

        Ok(results)
    }
}
