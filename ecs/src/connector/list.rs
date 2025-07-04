use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::addr::EcsResourceAddress;

use super::EcsConnector;

impl EcsConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        for region_name in &self.config.lock().await.enabled_regions {
            let client = self.get_or_init_client(region_name).await?;

            // List clusters
            let clusters_resp = client.list_clusters().send().await?;
            if let Some(cluster_arns) = clusters_resp.cluster_arns
                && !cluster_arns.is_empty()
            {
                // Get cluster names from ARNs
                let clusters_resp = client.describe_clusters().set_clusters(Some(cluster_arns)).send().await?;

                if let Some(clusters) = clusters_resp.clusters {
                    for cluster in clusters {
                        if let Some(cluster_name) = cluster.cluster_name {
                            // Add cluster to results
                            results
                                .push(EcsResourceAddress::Cluster(region_name.to_string(), cluster_name.clone()).to_path_buf());

                            // List services in the cluster
                            let services_resp = client.list_services().cluster(&cluster_name).send().await?;

                            if let Some(service_arns) = services_resp.service_arns
                                && !service_arns.is_empty()
                            {
                                // Get service details
                                let describe_services_resp = client
                                    .describe_services()
                                    .cluster(&cluster_name)
                                    .set_services(Some(service_arns))
                                    .send()
                                    .await?;

                                if let Some(services) = describe_services_resp.services {
                                    for service in services {
                                        if let Some(service_name) = service.service_name {
                                            results.push(
                                                EcsResourceAddress::Service(
                                                    region_name.to_string(),
                                                    cluster_name.clone(),
                                                    service_name,
                                                )
                                                .to_path_buf(),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // List task definitions (not cluster-specific)
            let task_definitions_resp = client.list_task_definition_families().send().await?;
            if let Some(families) = task_definitions_resp.families {
                for family in families {
                    // Get latest active revision for each family
                    let task_defs_resp = client
                        .list_task_definitions()
                        .family_prefix(&family)
                        .sort("DESC".into())
                        .status("ACTIVE".into())
                        .max_results(1)
                        .send()
                        .await?;

                    if let Some(task_def_arns) = task_defs_resp.task_definition_arns {
                        for task_def_arn in task_def_arns {
                            // Extract family:revision format from ARN
                            if let Some(task_def_id) = task_def_arn.split('/').next_back().map(String::from) {
                                results.push(
                                    EcsResourceAddress::TaskDefinition(region_name.to_string(), task_def_id).to_path_buf(),
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}
