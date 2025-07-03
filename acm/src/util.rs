use anyhow::Result;

/// Utility functions for ACM connector operations
pub fn encode_certificate_arn(arn: &str) -> String {
    urlencoding::encode(arn).to_string()
}

pub fn decode_certificate_arn(encoded: &str) -> Result<String> {
    Ok(urlencoding::decode(encoded)?.to_string())
}

/// Extract the certificate ID from a certificate ARN
pub fn extract_certificate_id(arn: &str) -> Option<String> {
    // ACM ARN format: arn:aws:acm:region:account:certificate/certificate-id
    arn.split('/').last().map(|s| s.to_string())
}

/// Extract the region from a certificate ARN
pub fn extract_region_from_arn(arn: &str) -> Option<String> {
    // ACM ARN format: arn:aws:acm:region:account:certificate/certificate-id
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 4 {
        Some(parts[3].to_string())
    } else {
        None
    }
}
