use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, error_util::invalid_addr_path};

#[derive(Debug, Clone)]
pub enum S3ResourceAddress {
    Bucket { region: String, name: String },
    // Object(Region, BucketName, ObjectName),
}

impl ResourceAddress for S3ResourceAddress {
    fn to_path_buf(&self) -> PathBuf {
        match &self {
            S3ResourceAddress::Bucket { region, name } => PathBuf::from(format!("aws/s3/{}/buckets/{}.ron", region, name)),
        }
    }

    fn from_path(path: &Path) -> Result<Self, anyhow::Error> {
        let path_components: Vec<&str> = path
            .components()
            .into_iter()
            .map(|s| s.as_os_str().to_str().unwrap())
            .collect();

        match path_components[..] {
            ["aws", "s3", region, "buckets", name] if name.ends_with(".ron") => {
                let name = name.strip_suffix(".ron").unwrap().to_string();
                Ok(S3ResourceAddress::Bucket {
                    region: region.to_string(),
                    name,
                })
            }
            _ => Err(invalid_addr_path(path))
        }
    }
}
