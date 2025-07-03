use std::path::Path;

use anyhow::{Result, bail};
use autoschematic_core::{
    connector::ConnectorOp,
    connector::{OpPlanOutput, ResourceAddress},
    connector_op,
    util::RON,
};

use anyhow::Context;

use crate::{
    addr::AcmResourceAddress,
    op::AcmConnectorOp,
    resource::{AcmCertificate, AcmResource},
    tags::{self, Tags},
};

use super::AcmConnector;

impl AcmConnector {
    pub async fn do_plan(&self, addr: &Path, current: Option<String>, desired: Option<String>) -> Result<Vec<OpPlanOutput>> {
        let addr = AcmResourceAddress::from_path(addr)?;
        let mut ops = Vec::new();

        match addr {
            AcmResourceAddress::Certificate { .. } => {
                match (current, desired) {
                    (None, Some(desired_str)) => {
                        // Certificate doesn't exist, need to create it
                        let desired_cert: AcmCertificate = RON.from_str(&desired_str)?;

                        ops.push(connector_op!(
                            AcmConnectorOp::RequestCertificate(desired_cert.clone()),
                            format!(
                                "Request new ACM certificate for domain '{}' using {} validation",
                                desired_cert.domain_name, desired_cert.validation_method
                            )
                        ));
                    }
                    (Some(current_str), Some(desired_str)) => {
                        // Certificate exists, check for differences
                        let current_cert: AcmCertificate = RON.from_str(&current_str)?;
                        let desired_cert: AcmCertificate = RON.from_str(&desired_str)?;

                        // Check if tags need updating
                        let (untag_keys, new_tags) = tags::tag_diff(&current_cert.tags, &desired_cert.tags)?;

                        if !untag_keys.is_empty() {
                            ops.push(connector_op!(
                                AcmConnectorOp::RemoveTags(untag_keys.clone()),
                                format!("Remove tags: {}", untag_keys.join(", "))
                            ));
                        }

                        if !new_tags.is_empty() {
                            let new_tags_struct = Tags::from(Some(new_tags.clone()));
                            let tag_descriptions: Vec<String> = new_tags
                                .iter()
                                .map(|tag| format!("{}={}", tag.key(), tag.value().unwrap_or_default()))
                                .collect();
                            ops.push(connector_op!(
                                AcmConnectorOp::AddTags(new_tags_struct),
                                format!("Add/update tags: {}", tag_descriptions.join(", "))
                            ));
                        }

                        // Note: Most certificate properties cannot be changed after creation
                        // If domain name, SANs, or validation method differ, we'd need to recreate
                        if current_cert.domain_name != desired_cert.domain_name
                            || current_cert.subject_alternative_names != desired_cert.subject_alternative_names
                            || current_cert.validation_method != desired_cert.validation_method
                        {
                            bail!(
                                "Cannot modify a certificate's domain, alternative names, or validation method - you must delete and recreate to continue."
                            );
                        }
                    }
                    (Some(_), None) => {
                        ops.push(connector_op!(
                            AcmConnectorOp::DeleteCertificate,
                            "Delete ACM certificate".to_string()
                        ));
                    }
                    (None, None) => {
                        // Nothing to do
                    }
                }
            }
        }

        Ok(ops)
    }
}
