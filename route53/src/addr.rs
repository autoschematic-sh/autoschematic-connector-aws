use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};


#[derive(Debug, Clone)]
pub enum Route53ResourceAddress {
    HostedZone(String),
    ResourceRecordSet(String, String, String), // (hosted_zone, name, type)
    HealthCheck(String),
}

impl ResourceAddress for Route53ResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            Route53ResourceAddress::HostedZone(name) => PathBuf::from(format!(
                "aws/route53/hosted_zones/{}/config.ron",
                name.strip_suffix(".").unwrap()
            )),
            Route53ResourceAddress::ResourceRecordSet(hosted_zone, name, r#type) => {
                PathBuf::from(format!(
                    "aws/route53/hosted_zones/{}/records/{}/{}.ron",
                    hosted_zone.strip_suffix(".").unwrap(),
                    r#type,
                    name.strip_suffix(".").unwrap(),
                ))
            }
            Route53ResourceAddress::HealthCheck(name) => PathBuf::from(format!(
                "aws/route53/health_checks/{}.ron",
                name.strip_suffix(".").unwrap(),
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path
            .components()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match path_components[..] {
            ["aws", "route53", "hosted_zones", name, "config.ron"] => {
                let mut name = name.to_string();
                name.push('.');
                Ok(Route53ResourceAddress::HostedZone(name))
            }
            ["aws", "route53", "health_checks", name] if name.ends_with(".ron") => {
                let mut name = name.strip_suffix(".ron").unwrap().to_string();

                name.push('.');
                Ok(Route53ResourceAddress::HealthCheck(name))
            }
            ["aws", "route53", "hosted_zones", hosted_zone, "records", r#type, name]
                if name.ends_with(".ron") =>
            {
                let mut hosted_zone = hosted_zone.to_string();
                hosted_zone.push('.');
                let mut name = name.strip_suffix(".ron").unwrap().to_string();
                name.push('.');

                Ok(Route53ResourceAddress::ResourceRecordSet(
                    hosted_zone,
                    name,
                    r#type.to_string(),
                ))
            }
            _ => Err(invalid_addr_path(path))
        }
    }
}
