use std::path::Path;

use anyhow::{Result, bail};
use autoschematic_core::{
    connector::{OpExecOutput, ResourceAddress},
    util::RON,
};

use crate::{
    addr::AcmResourceAddress,
    op::AcmConnectorOp,
};

use super::AcmConnector;

impl AcmConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput> {
        let Some(account_id) = self.account_id.read().await.clone() else {
            bail!("Account ID not set");
        };

        let addr = AcmResourceAddress::from_path(addr)?;

        let certificate_arn = addr.to_certificate_arn(&account_id);

        let op: AcmConnectorOp = RON.from_str(op)?;

        match addr {
            AcmResourceAddress::Certificate { region, .. } => {
                let client = self.get_or_init_client(&region).await.unwrap();
                match op {
                    AcmConnectorOp::RequestCertificate(cert_config) => {
                        // Request a new certificate
                        let mut request = client.request_certificate().domain_name(&cert_config.domain_name);

                        // Add subject alternative names if provided
                        if !cert_config.subject_alternative_names.is_empty() {
                            request =
                                request.set_subject_alternative_names(Some(cert_config.subject_alternative_names.clone()));
                        }

                        // Set validation method
                        let validation_method = match cert_config.validation_method.as_str() {
                            "DNS" => aws_sdk_acm::types::ValidationMethod::Dns,
                            "EMAIL" => aws_sdk_acm::types::ValidationMethod::Email,
                            _ => aws_sdk_acm::types::ValidationMethod::Dns, // Default to DNS
                        };
                        request = request.validation_method(validation_method);

                        // Set certificate transparency logging preference
                        if let Some(ct_pref) = &cert_config.certificate_transparency_logging_preference {
                            let ct_logging = match ct_pref.as_str() {
                                "ENABLED" => aws_sdk_acm::types::CertificateTransparencyLoggingPreference::Enabled,
                                "DISABLED" => aws_sdk_acm::types::CertificateTransparencyLoggingPreference::Disabled,
                                _ => aws_sdk_acm::types::CertificateTransparencyLoggingPreference::Enabled,
                            };
                            request = request.options(
                                aws_sdk_acm::types::CertificateOptions::builder()
                                    .certificate_transparency_logging_preference(ct_logging)
                                    .build(),
                            );
                        }

                        // Add domain validation options if provided
                        if !cert_config.validation_options.is_empty() {
                            let domain_validation_options: Vec<aws_sdk_acm::types::DomainValidationOption> = cert_config
                                .validation_options
                                .iter()
                                .map(|vo| {
                                    let mut builder =
                                        aws_sdk_acm::types::DomainValidationOption::builder().domain_name(&vo.domain_name);
                                    if let Some(validation_domain) = &vo.validation_domain {
                                        builder = builder.validation_domain(validation_domain);
                                    }
                                    builder.build().unwrap()
                                })
                                .collect();
                            request = request.set_domain_validation_options(Some(domain_validation_options));
                        }

                        // Add tags if provided
                        if cert_config.tags.len() > 0 {
                            let tags: Option<Vec<aws_sdk_acm::types::Tag>> = cert_config.tags.into();
                            if let Some(tags) = tags {
                                request = request.set_tags(Some(tags));
                            }
                        }

                        let response = request.send().await?;

                        if let Some(cert_arn) = response.certificate_arn {
                            let mut outputs = std::collections::HashMap::new();
                            outputs.insert("certificate_arn".to_string(), Some(cert_arn));
                            Ok(OpExecOutput {
                                outputs: Some(outputs),
                                friendly_message: Some("Certificate requested successfully".to_string()),
                            })
                        } else {
                            Ok(OpExecOutput {
                                outputs: None,
                                friendly_message: Some("Certificate request completed".to_string()),
                            })
                        }
                    }
                    AcmConnectorOp::DeleteCertificate => {
                        // Delete the certificate
                        client.delete_certificate().certificate_arn(&certificate_arn).send().await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some("Certificate deleted successfully".to_string()),
                        })
                    }
                    AcmConnectorOp::AddTags(tags) => {
                        // Add tags to the certificate
                        let aws_tags: Option<Vec<aws_sdk_acm::types::Tag>> = tags.into();
                        if let Some(aws_tags) = aws_tags {
                            client
                                .add_tags_to_certificate()
                                .certificate_arn(&certificate_arn)
                                .set_tags(Some(aws_tags))
                                .send()
                                .await?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some("Tags added successfully".to_string()),
                        })
                    }
                    AcmConnectorOp::RemoveTags(tag_keys) => {
                        // Remove tags from the certificate
                        client
                            .remove_tags_from_certificate()
                            .certificate_arn(&certificate_arn)
                            .set_tags(Some(
                                tag_keys
                                    .iter()
                                    .map(|key| aws_sdk_acm::types::Tag::builder().key(key).build().unwrap())
                                    .collect(),
                            ))
                            .send()
                            .await?;

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some("Tags removed successfully".to_string()),
                        })
                    }
                    AcmConnectorOp::UpdateTags(old_tags, new_tags) => {
                        // This is a combination of remove and add operations
                        let (untag_keys, add_tags) = crate::tags::tag_diff(&old_tags, &new_tags)?;

                        // Remove old tags
                        if !untag_keys.is_empty() {
                            client
                                .remove_tags_from_certificate()
                                .certificate_arn(&certificate_arn)
                                .set_tags(Some(
                                    untag_keys
                                        .iter()
                                        .map(|key| aws_sdk_acm::types::Tag::builder().key(key).build().unwrap())
                                        .collect(),
                                ))
                                .send()
                                .await?;
                        }

                        // Add new tags
                        if !add_tags.is_empty() {
                            client
                                .add_tags_to_certificate()
                                .certificate_arn(&certificate_arn)
                                .set_tags(Some(add_tags))
                                .send()
                                .await?;
                        }

                        Ok(OpExecOutput {
                            outputs: None,
                            friendly_message: Some("Tags updated successfully".to_string()),
                        })
                    }
                }
            }
        }
    }
}
