use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::{addr::RdsResourceAddress, client_cache, resource::RdsResource};

use super::RdsConnector;

impl RdsConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::new();
        for region in &self.config.lock().await.enabled_regions {
            let client = self.get_or_init_client(region).await?;

            // List instances
            let mut instances = client.describe_db_instances().into_paginator().send();
            while let Some(instances) = instances.next().await {
                let Some(instances) = instances?.db_instances else {
                    break;
                };

                for instance in instances {
                    if let Some(name) = instance.db_name {
                        results.push(
                            RdsResourceAddress::DBInstance {
                                region: region.into(),
                                id: name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            // List clusters
            let mut clusters = client.describe_db_clusters().into_paginator().send();
            while let Some(clusters) = clusters.next().await {
                let Some(clusters) = clusters?.db_clusters else {
                    break;
                };

                for cluster in clusters {
                    if let Some(name) = cluster.database_name {
                        results.push(
                            RdsResourceAddress::DBCluster {
                                region: region.into(),
                                id: name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            // List subnet_groups
            let mut subnet_groups = client.describe_db_subnet_groups().into_paginator().send();
            while let Some(subnet_groups) = subnet_groups.next().await {
                let Some(subnet_groups) = subnet_groups?.db_subnet_groups else {
                    break;
                };

                for subnet_group in subnet_groups {
                    if let Some(name) = subnet_group.db_subnet_group_name {
                        results.push(
                            RdsResourceAddress::DBSubnetGroup {
                                region: region.into(),
                                name: name,
                            }
                            .to_path_buf(),
                        );
                    }
                }
            }

            // List subnet_groups
            let mut parameter_groups = client.describe_db_parameter_groups().into_paginator().send();
            while let Some(parameter_groups) = parameter_groups.next().await {
                let Some(parameter_groups) = parameter_groups?.db_parameter_groups else {
                    break;
                };

                for parameter_group in parameter_groups {
                    if let Some(name) = parameter_group.db_parameter_group_name {
                        results.push(
                            RdsResourceAddress::DBParameterGroup {
                                region: region.into(),
                                name: name,
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
