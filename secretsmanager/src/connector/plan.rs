use std::path::Path;

use autoschematic_core::{
    connector::{OpPlanOutput, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use autoschematic_core::connector::ConnectorOp;

use crate::resource::Secret;

use super::{SecretsManagerConnector, SecretsManagerConnectorOp, SecretsManagerResourceAddress};

impl SecretsManagerConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = SecretsManagerResourceAddress::from_path(addr)?;

        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            SecretsManagerResourceAddress::Secret { region, name } => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_secret_str)) => {
                        // Create a new secret
                        let new_secret: Secret = RON.from_str(&new_secret_str)?;
                        Ok(vec![connector_op!(
                            SecretsManagerConnectorOp::CreateSecret(new_secret),
                            format!("Create new secret '{}'", name)
                        )])
                    }
                    (Some(_), None) => {
                        // Delete an existing secret
                        Ok(vec![connector_op!(
                            SecretsManagerConnectorOp::DeleteSecret {
                                recovery_window_in_days: Some(30), // Default to 30-day recovery window
                                force_delete_without_recovery: None,
                            },
                            format!("Delete secret '{}'", name)
                        )])
                    }
                    (Some(old_secret_str), Some(new_secret_str)) => {
                        // Compare old and new secret to determine what needs to be updated
                        let old_secret: Secret = RON.from_str(&old_secret_str)?;
                        let new_secret: Secret = RON.from_str(&new_secret_str)?;
                        let mut ops = Vec::new();

                        // Check for description changes
                        if old_secret.description != new_secret.description {
                            if let Some(description) = &new_secret.description {
                                ops.push(connector_op!(
                                    SecretsManagerConnectorOp::UpdateSecretDescription {
                                        description: description.clone(),
                                    },
                                    format!("Update description for secret '{}'", name)
                                ));
                            }
                        }

                        // Check for KMS key ID changes
                        if old_secret.kms_key_id != new_secret.kms_key_id {
                            if let Some(kms_key_id) = &new_secret.kms_key_id {
                                ops.push(connector_op!(
                                    SecretsManagerConnectorOp::UpdateSecretKmsKeyId {
                                        kms_key_id: kms_key_id.clone(),
                                    },
                                    format!("Update KMS key for secret '{}'", name)
                                ));
                            }
                        }

                        // Check for secret value changes
                        // if old_secret.secret_ref != new_secret.secret_ref {
                        //     if let Some(secret_ref) = new_secret.secret_ref {
                        //         ops.push(connector_op!(
                        //             SecretsManagerConnectorOp::UpdateSecretValue {
                        //                 secret_ref: secret_ref,
                        //                 client_request_token: None,
                        //             },
                        //             format!("Update value for secret '{}'", secret_name)
                        //         ));
                        // } else {
                        if let Some(secret_ref) = new_secret.secret_ref {
                            let secret_value = self
                                .get_or_init_client(&region)
                                .await?
                                .get_secret_value()
                                .secret_id(&name)
                                .send()
                                .await;
                            // TODO something something compare the secret value, you know...
                        }
                        // }
                        // }

                        if old_secret.policy_document != new_secret.policy_document {
                            let diff =
                                diff_ron_values(&old_secret.policy_document, &old_secret.policy_document).unwrap_or_default();

                            ops.push(connector_op!(
                                SecretsManagerConnectorOp::SetSecretPolicy {
                                    policy_document:     new_secret.policy_document,
                                    block_public_policy: None,
                                },
                                format!("Update policy for secret '{}'\n{}", name, diff)
                            ))
                        }

                        // Check for tag changes
                        if old_secret.tags != new_secret.tags {
                            let diff = diff_ron_values(&old_secret.tags, &new_secret.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                SecretsManagerConnectorOp::UpdateSecretTags(old_secret.tags, new_secret.tags,),
                                format!("Update tags for secret '{}'\n{}", name, diff)
                            ));
                        }

                        Ok(ops)
                    }
                }
            } // Some(SecretsManagerResourceAddress::SecretPolicy(region, secret_name)) => {
              //     match (current, desired) {
              //         (None, None) => Ok(vec![]),
              //         (None, Some(new_policy_str)) => {
              //             // Create a new policy
              //             let new_policy: SecretPolicy = RON.from_str(&new_policy_str)?;
              //             Ok(vec![connector_op!(
              //                 SecretsManagerConnectorOp::SetSecretPolicy {
              //                     policy_document: new_policy.policy_document,
              //                     block_public_policy: None,
              //                 },
              //                 format!("Set policy for secret '{}'", secret_name)
              //             )])
              //         }
              //         (Some(_), None) => {
              //             // Delete an existing policy
              //             Ok(vec![connector_op!(
              //                 SecretsManagerConnectorOp::DeleteSecretPolicy,
              //                 format!("Delete policy for secret '{}'", secret_name)
              //             )])
              //         }
              //         (Some(old_policy_str), Some(new_policy_str)) => {
              //             // Compare old and new policy to determine if it needs to be updated
              //             let old_policy: SecretPolicy = RON.from_str(&old_policy_str)?;
              //             let new_policy: SecretPolicy = RON.from_str(&new_policy_str)?;

              //             if old_policy.policy_document != new_policy.policy_document {
              //                 let diff = diff_ron_values(
              //                     &old_policy.policy_document,
              //                     &new_policy.policy_document,
              //                 )
              //                 .unwrap_or_default();

              //                 Ok(vec![connector_op!(
              //                     SecretsManagerConnectorOp::SetSecretPolicy {
              //                         policy_document: new_policy.policy_document,
              //                         block_public_policy: None,
              //                     },
              //                     format!("Update policy for secret '{}'\n{}", secret_name, diff)
              //                 )])
              //             } else {
              //                 // No changes needed
              //                 Ok(vec![])
              //             }
              //         }
              //     }
              // }
        }
    }
}
