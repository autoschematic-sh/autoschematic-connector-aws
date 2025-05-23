use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::{addr::S3ResourceAddress, util};

use super::S3Connector;

impl S3Connector {
    pub async fn do_list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        let path_components: Vec<&str> = subpath.components().map(|s| s.as_os_str().to_str().unwrap()).collect();

        let config = self.config.lock().await;

        match &path_components[..] {
            ["aws", "s3", region_name, prefix @ ..] => {
                let region_name = region_name.to_string();
                if config.enabled_regions.contains(&region_name) {
                    let prefix = if !prefix.is_empty() { Some(prefix.join("/")) } else { None };
                    let client = self.get_or_init_client(&region_name).await.unwrap();
                    let bucket_names = util::list_buckets(client, &region_name, prefix).await?;
                    for bucket_name in bucket_names {
                        results.push(
                            S3ResourceAddress::Bucket {
                                region: region_name.clone(),
                                name: bucket_name,
                            }
                            .to_path_buf(),
                        );
                    }
                } else {
                    return Ok(Vec::new());
                }
            }

            _ => {
                for region_name in &config.enabled_regions {
                    let client = self.get_or_init_client(region_name).await.unwrap();
                    let bucket_names = util::list_buckets(client, region_name, None).await?;
                    for bucket_name in bucket_names {
                        results.push(
                            S3ResourceAddress::Bucket {
                                region: region_name.clone(),
                                name: bucket_name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }
        }

        Ok(results)
    }
}
