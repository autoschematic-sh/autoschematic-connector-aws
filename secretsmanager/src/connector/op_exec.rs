use std::{collections::HashMap, path::Path};

use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress}, connector_util::read_mounted_secret, error_util::invalid_op
};

use crate::tags;

use super::{SecretsManagerConnector, SecretsManagerConnectorOp, SecretsManagerResourceAddress};

impl SecretsManagerConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = SecretsManagerResourceAddress::from_path(addr)?;
        let op = SecretsManagerConnectorOp::from_str(op)?;

        match &addr {
            SecretsManagerResourceAddress::Secret { region, name } => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    SecretsManagerConnectorOp::CreateSecret(secret) => {
                        // Build the create request
                        let mut request = client.create_secret().name(name);

                        if let Some(description) = &secret.description {
                            request = request.description(description);
                        }

                        if let Some(kms_key_id) = &secret.kms_key_id {
                            request = request.kms_key_id(kms_key_id);
                        }

                        if let Some(secret_ref) = &secret.secret_ref {
                            request = request.secret_string(read_mounted_secret(&self.prefix, secret_ref)?);
                        }

                        // Add tags if provided
                        if secret.tags.len() > 0 {
                            let aws_tags = secret.tags.to_vec()?;
                            request = request.set_tags(Some(aws_tags));
                        }

                        // Send the request
                        let result = request.send().await?;

                        let mut outputs = HashMap::new();
                        // if let Some(name) = result.name {
                        //     outputs.insert(String::from("secret_name"), Some(name));
                        // }
                        if let Some(arn) = result.arn {
                            outputs.insert(String::from("arn"), Some(arn));
                        }

                        Ok(OpExecOutput {
                            outputs: Some(outputs),
                            friendly_message: Some(format!("Created secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::UpdateSecretDescription { description } => {
                        // Update the secret description
                        let result = client.update_secret().secret_id(name).description(description).send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated description for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::UpdateSecretValue {
                        secret_ref,
                        client_request_token,
                    } => {
                        // Update the secret value
                        let mut request = client.put_secret_value().secret_id(name).secret_string(
                            autoschematic_core::connector_util::read_mounted_secret(&self.prefix, &secret_ref)?,
                        );

                        if let Some(token) = client_request_token {
                            request = request.client_request_token(token);
                        }

                        let result = request.send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated value for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::UpdateSecretTags(old_tags, new_tags) => {
                        // Calculate tag differences
                        let (untag_keys, new_tagset) = tags::tag_diff(&old_tags, &new_tags)?;

                        // Remove tags
                        if !untag_keys.is_empty() {
                            client
                                .untag_resource()
                                .secret_id(name)
                                .set_tag_keys(Some(untag_keys))
                                .send()
                                .await?;
                        }

                        // Add new tags
                        if !new_tagset.is_empty() {
                            client
                                .tag_resource()
                                .secret_id(name)
                                .set_tags(Some(new_tagset))
                                .send()
                                .await?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated tags for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::UpdateSecretKmsKeyId { kms_key_id } => {
                        // Update the KMS key ID used to encrypt the secret
                        let result = client.update_secret().secret_id(name).kms_key_id(kms_key_id).send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Updated KMS key for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::DeleteSecret {
                        recovery_window_in_days,
                        force_delete_without_recovery,
                    } => {
                        // Delete the secret
                        let mut request = client.delete_secret().secret_id(name);

                        if let Some(window) = recovery_window_in_days {
                            request = request.recovery_window_in_days(window);
                        }

                        if let Some(force) = force_delete_without_recovery {
                            if force {
                                request = request.force_delete_without_recovery(true);
                            }
                        }

                        let result = request.send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Deleted secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::RestoreSecret => {
                        // Restore a previously deleted secret
                        let result = client.restore_secret().secret_id(name).send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Restored secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::RotateSecret {
                        rotation_lambda_arn,
                        rotation_rules,
                    } => {
                        // Configure rotation for the secret
                        let mut request = client
                            .rotate_secret()
                            .secret_id(name)
                            .rotation_lambda_arn(rotation_lambda_arn);

                        // Create rotation rules
                        let mut rotation_rules_builder = aws_sdk_secretsmanager::types::RotationRulesType::builder();

                        if let Some(days) = rotation_rules.automatically_after_days {
                            rotation_rules_builder = rotation_rules_builder.automatically_after_days(days);
                        }

                        if let Some(duration) = &rotation_rules.duration {
                            rotation_rules_builder = rotation_rules_builder.duration(duration);
                        }

                        if let Some(expression) = &rotation_rules.schedule_expression {
                            rotation_rules_builder = rotation_rules_builder.schedule_expression(expression);
                        }

                        request = request.rotation_rules(rotation_rules_builder.build());

                        let result = request.send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Configured rotation for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::SetSecretPolicy {
                        policy_document,
                        block_public_policy,
                    } => {
                        let policy_text = serde_json::to_string(&policy_document)?;

                        // Set the resource policy
                        let mut request = client.put_resource_policy().secret_id(name).resource_policy(policy_text);

                        if let Some(block) = block_public_policy {
                            request = request.block_public_policy(block);
                        }

                        let result = request.send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Set policy for secret '{}'", name)),
                        })
                    }
                    SecretsManagerConnectorOp::DeleteSecretPolicy => {
                        // Delete the resource policy
                        let result = client.delete_resource_policy().secret_id(name).send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some(format!("Deleted policy for secret '{}'", name)),
                        })
                    }
                    op => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
