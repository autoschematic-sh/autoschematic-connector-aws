use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum AcmResourceAddress {
    Certificate { region: String, certificate_id: String },
}

impl AcmResourceAddress {
    /// Convert certificate ID to full ARN for AWS API calls
    pub fn to_certificate_arn(&self, account_id: &str) -> String {
        match self {
            AcmResourceAddress::Certificate { region, certificate_id } => {
                format!("arn:aws:acm:{}:{}:certificate/{}", region, account_id, certificate_id)
            }
        }
    }

    /// Extract certificate ID from a certificate ARN
    pub fn from_certificate_arn(arn: &str) -> Option<String> {
        // ACM ARN format: arn:aws:acm:region:account:certificate/certificate-id
        arn.split('/').last().map(|s| s.to_string())
    }

    /// Get the region from this address
    pub fn region(&self) -> &str {
        match self {
            AcmResourceAddress::Certificate { region, .. } => region,
        }
    }
}

impl ResourceAddress for AcmResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            AcmResourceAddress::Certificate { region, certificate_id } => {
                PathBuf::from(format!("aws/acm/{}/certificates/{}.ron", region, certificate_id))
            }
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        match &path_components[..] {
            ["aws", "acm", region, "certificates", certificate_file] if certificate_file.ends_with(".ron") => {
                let certificate_id = certificate_file.strip_suffix(".ron").unwrap().to_string();
                Ok(AcmResourceAddress::Certificate {
                    region: region.to_string(),
                    certificate_id,
                })
            }
            _ => Err(invalid_addr_path(path)),
        }
    }
}
