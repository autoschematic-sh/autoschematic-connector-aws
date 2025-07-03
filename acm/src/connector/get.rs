use std::path::Path;

use anyhow::{Result, bail};
use autoschematic_core::{
    connector::{GetResourceOutput, Resource},
    get_resource_output,
};

use anyhow::Context;
use std::collections::HashMap;

use crate::{
    addr::AcmResourceAddress,
    resource::{AcmCertificate, AcmResource, ValidationOption},
    tags::Tags,
    util::extract_certificate_id,
};
use autoschematic_core::connector::ResourceAddress;

use super::AcmConnector;

impl AcmConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>> {
        let Some(account_id) = self.account_id.read().await.clone() else {
            bail!("Account ID not set");
        };
        let addr = AcmResourceAddress::from_path(addr)?;

        match &addr {
            AcmResourceAddress::Certificate { region, .. } => {
                let client = self.get_or_init_client(&region).await?;
                let certificate_arn = addr.to_certificate_arn(&account_id);

                let describe_result = client.describe_certificate().certificate_arn(&certificate_arn).send().await;

                match describe_result {
                    Ok(response) => {
                        if let Some(certificate) = response.certificate {
                            // Get certificate tags
                            let tags_result = client
                                .list_tags_for_certificate()
                                .certificate_arn(&certificate_arn)
                                .send()
                                .await;

                            let tags = match tags_result {
                                Ok(tags_response) => Tags::from(tags_response.tags),
                                Err(_) => Tags::default(),
                            };

                            let domain_name = certificate.domain_name().unwrap_or_default().to_string();
                            let subject_alternative_names = certificate
                                .subject_alternative_names()
                                .iter()
                                .map(|s| s.to_string())
                                .collect();

                            let validation_method = certificate
                                .domain_validation_options
                                .clone()
                                .and_then(|v| {
                                    v.first()
                                        .and_then(|v| v.validation_method.clone().map(|v| v.as_str().to_string()))
                                })
                                .unwrap_or("DNS".into());

                            let validation_options = certificate
                                .domain_validation_options()
                                .iter()
                                .map(|dvo| ValidationOption {
                                    domain_name: dvo.domain_name.clone(),
                                    validation_domain: dvo.validation_domain().map(|s| s.to_string()),
                                })
                                .collect();

                            let certificate_transparency_logging_preference = certificate
                                .options()
                                .and_then(|opts| opts.certificate_transparency_logging_preference())
                                .map(|pref| pref.as_str().to_string());

                            let acm_certificate = AcmCertificate {
                                domain_name,
                                subject_alternative_names,
                                validation_method,
                                validation_options,
                                certificate_transparency_logging_preference,
                                tags,
                            };
                            let certificate_arn = certificate.certificate_arn.unwrap_or_default();
                            let certificate_id = extract_certificate_id(&certificate_arn).unwrap_or_default();

                            let resource = AcmResource::Certificate(acm_certificate);
                            get_resource_output!(
                                resource,
                                [
                                    (String::from("certificate_id"), certificate_id),
                                    (String::from("certificate_arn"), certificate_arn),
                                    (
                                        String::from("certificate_domain"),
                                        certificate.domain_name.unwrap_or_default()
                                    )
                                ]
                            )
                        } else {
                            Ok(None)
                        }
                    }
                    Err(err) => {
                        // Check if it's a ResourceNotFoundException
                        if err.to_string().contains("ResourceNotFoundException") {
                            Ok(None)
                        } else {
                            Err(err.into())
                        }
                    }
                }
            }
        }
    }
}
