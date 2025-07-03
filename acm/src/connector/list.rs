use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use autoschematic_core::{connector::ResourceAddress, glob::addr_matches_filter};

use crate::{addr::AcmResourceAddress, util::extract_certificate_id};

use super::AcmConnector;

impl AcmConnector {
    pub async fn do_list(&self, subpath: &Path) -> Result<Vec<PathBuf>> {
        let mut results = Vec::<PathBuf>::new();
        let config = self.config.read().await;

        for region in &config.enabled_regions {
            if !addr_matches_filter(&PathBuf::from(format!("aws/acm/{}", region)), subpath) {
                continue;
            }

            let client = self.get_or_init_client(region).await.unwrap();

            let mut next_token: Option<String> = None;
            let mut request = client.list_certificates();
            if let Some(token) = &next_token {
                request = request.next_token(token);
            }

            let response = request.send().await?;

            if let Some(cert_list) = response.certificate_summary_list {
                for cert in cert_list {
                    let Some(certificate_id) = cert.certificate_arn.and_then(|arn| extract_certificate_id(&arn)) else {
                        bail!("Cert has no ARN?");
                    };
                    results.push(
                        AcmResourceAddress::Certificate {
                            region: region.into(),
                            certificate_id,
                        }
                        .to_path_buf(),
                    );
                }
            }

            next_token = response.next_token;
            if next_token.is_none() {
                break;
            }
        }

        Ok(results)
    }
}
