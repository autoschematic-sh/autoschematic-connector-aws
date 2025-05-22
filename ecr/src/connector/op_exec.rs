use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress}, error::{AutoschematicError, AutoschematicErrorType}, error_util::invalid_op, op_exec_output
};

use crate::{addr::EcrResourceAddress, op::EcrConnectorOp, op_impl};

use super::EcrConnector;

impl EcrConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = EcrResourceAddress::from_path(addr)?;
        let op = EcrConnectorOp::from_str(op)?;

        match &addr {
            EcrResourceAddress::Repository { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EcrConnectorOp::CreateRepository(repo) => op_impl::create_repository(&client, &repo).await,
                    EcrConnectorOp::UpdateRepositoryTags(old_tags, new_tags) => {
                        op_impl::update_repository_tags(&client, &name, &old_tags, &new_tags).await
                    }
                    EcrConnectorOp::UpdateImageTagMutability { image_tag_mutability } => {
                        op_impl::update_image_tag_mutability(&client, &name, &image_tag_mutability).await
                    }
                    EcrConnectorOp::UpdateImageScanningConfiguration { scan_on_push } => {
                        op_impl::update_image_scanning_configuration(&client, &name, scan_on_push).await
                    }
                    EcrConnectorOp::UpdateEncryptionConfiguration {
                        encryption_configuration,
                    } => op_impl::update_encryption_configuration(&client, &name, encryption_configuration).await,
                    EcrConnectorOp::DeleteRepository { force } => op_impl::delete_repository(&client, &name, force).await,
                    EcrConnectorOp::TagImage {
                        source_image_digest,
                        image_tag,
                    } => op_impl::tag_image(&client, &name, &source_image_digest, &image_tag).await,
                    EcrConnectorOp::UntagImage { image_tag } => op_impl::untag_image(&client, &name, &image_tag).await,
                    EcrConnectorOp::BatchDeleteImages { image_ids } => {
                        op_impl::batch_delete_images(&client, &name, &image_ids).await
                    }
                    _ => bail!("Invalid operation for repository resource"),
                }
            }
            EcrResourceAddress::RepositoryPolicy { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EcrConnectorOp::SetRepositoryPolicy { policy_document } => {
                        op_impl::set_repository_policy(&client, &name, &policy_document).await
                    }
                    EcrConnectorOp::DeleteRepositoryPolicy => op_impl::delete_repository_policy(&client, &name).await,
                    _ => bail!("Invalid operation for repository policy resource"),
                }
            }
            EcrResourceAddress::LifecyclePolicy { region, name } => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EcrConnectorOp::SetLifecyclePolicy { lifecycle_policy_text } => {
                        op_impl::set_lifecycle_policy(&client, &name, &lifecycle_policy_text).await
                    }
                    EcrConnectorOp::DeleteLifecyclePolicy => op_impl::delete_lifecycle_policy(&client, &name).await,
                    _ => bail!("Invalid operation for lifecycle policy resource"),
                }
            }
            EcrResourceAddress::RegistryPolicy { region } => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EcrConnectorOp::SetRegistryPolicy { policy_document } => {
                        op_impl::set_registry_policy(&client, &policy_document).await
                    }
                    EcrConnectorOp::DeleteRegistryPolicy => op_impl::delete_registry_policy(&client).await,
                    _ => bail!("Invalid operation for registry policy resource"),
                }
            }
            EcrResourceAddress::PullThroughCacheRule { region, prefix } => {
                let client = self.get_or_init_client(&region).await?;

                match op {
                    EcrConnectorOp::CreatePullThroughCacheRule {
                        upstream_registry_url,
                        credential_arn,
                    } => {
                        op_impl::create_pull_through_cache_rule(
                            &client,
                            &prefix,
                            &upstream_registry_url,
                            credential_arn.clone(),
                        )
                        .await
                    }
                    EcrConnectorOp::DeletePullThroughCacheRule {} => {
                        op_impl::delete_pull_through_cache_rule(&client, &prefix).await
                    }
                    _ => return Err(invalid_op(&addr, &op))
                }
            }
        }
    }
}
