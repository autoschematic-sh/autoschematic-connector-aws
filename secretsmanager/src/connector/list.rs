use std::path::{Path, PathBuf};

use autoschematic_core::{connector::ResourceAddress, glob::addr_matches_filter};

use super::{SecretsManagerConnector, SecretsManagerResourceAddress};

impl SecretsManagerConnector {
    pub async fn do_list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        let config = self.config.read().await;

        for region_name in &config.enabled_regions {
            if !addr_matches_filter(&PathBuf::from(format!("aws/secretsmanager/{region_name}")), subpath) {
                continue;
            }

            let client = self.get_or_init_client(region_name).await?;

            // List all secrets in the region
            let mut next_token: Option<String> = None;

            loop {
                let mut list_secrets_request = client.list_secrets();

                if let Some(token) = next_token {
                    list_secrets_request = list_secrets_request.next_token(token);
                }

                let secrets_resp = list_secrets_request.send().await?;

                if let Some(secrets) = secrets_resp.secret_list {
                    for secret in secrets {
                        if let Some(secret_name) = secret.name {
                            // Add the secret to results
                            results.push(
                                SecretsManagerResourceAddress::Secret {
                                    region: region_name.to_string(),
                                    name:   secret_name.clone(),
                                }
                                .to_path_buf(),
                            );
                        }
                    }
                }

                // Check if there are more secrets to fetch
                next_token = secrets_resp.next_token;
                if next_token.is_none() {
                    break;
                }
            }
        }

        Ok(results)
    }
}
