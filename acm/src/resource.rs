use autoschematic_core::connector::{Resource, ResourceAddress};
use autoschematic_core::util::{PrettyConfig, RON};
use documented::{Documented, DocumentedFields};
use serde::{Deserialize, Serialize};

use super::{addr::AcmResourceAddress, tags::Tags};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields)]
#[serde(deny_unknown_fields)]
/// An ACM certificate represents an SSL/TLS certificate managed by AWS Certificate Manager.
/// ACM certificates can be used with AWS services like CloudFront, Application Load Balancer, and API Gateway.
pub struct AcmCertificate {
    /// The primary domain name for the certificate (e.g., "example.com")
    pub domain_name: String,
    /// Additional domain names covered by this certificate (Subject Alternative Names)
    pub subject_alternative_names: Vec<String>,
    /// The validation method used for the certificate: "DNS" or "EMAIL"
    pub validation_method: String,
    /// Domain validation options specifying how each domain should be validated
    pub validation_options: Vec<ValidationOption>,
    /// Certificate transparency logging preference: "ENABLED" or "DISABLED"
    pub certificate_transparency_logging_preference: Option<String>,
    /// A set of Key: Value tags. Each key and value can only be a string.
    pub tags: Tags,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Documented, DocumentedFields)]
#[serde(deny_unknown_fields)]
/// Validation option for a domain in an ACM certificate
pub struct ValidationOption {
    /// The domain name to be validated
    pub domain_name: String,
    /// The domain to use for validation (if different from domain_name)
    pub validation_domain: Option<String>,
}

pub enum AcmResource {
    Certificate(AcmCertificate),
}

impl Resource for AcmResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = PrettyConfig::default().struct_names(true);
        match self {
            AcmResource::Certificate(certificate) => match RON.to_string_pretty(&certificate, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = AcmResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;
        match addr {
            AcmResourceAddress::Certificate { .. } => Ok(AcmResource::Certificate(RON.from_str(s)?)),
        }
    }
}
