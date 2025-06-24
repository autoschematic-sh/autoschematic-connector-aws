use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::addr::ElbResourceAddress;

use super::ElbConnector;

impl ElbConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();

        let config = self.config.lock().await;

        for region_name in &config.enabled_regions {
            let client = self.get_or_init_client(region_name).await?;

            // List Load Balancers
            let load_balancers_resp = client.describe_load_balancers().send().await?;
            if let Some(load_balancers) = load_balancers_resp.load_balancers {
                for lb in load_balancers {
                    if let Some(lb_name) = &lb.load_balancer_name {
                        results.push(ElbResourceAddress::LoadBalancer(region_name.clone(), lb_name.clone()).to_path_buf());

                        // List Listeners for each Load Balancer
                        if let Some(lb_arn) = &lb.load_balancer_arn {
                            let listeners_resp = client.describe_listeners().load_balancer_arn(lb_arn).send().await?;

                            if let Some(listeners) = listeners_resp.listeners {
                                for listener in listeners {
                                    if let Some(listener_id) = &listener.listener_arn {
                                        // Extract just the ID part from the ARN
                                        let listener_id_parts: Vec<&str> = listener_id.split('/').collect();
                                        let listener_id_short = listener_id_parts.last().unwrap_or(&"").to_string();

                                        results.push(
                                            ElbResourceAddress::Listener(
                                                region_name.clone(),
                                                lb_name.clone(),
                                                listener_id_short,
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

            // List Target Groups
            let target_groups_resp = client.describe_target_groups().send().await?;
            if let Some(target_groups) = target_groups_resp.target_groups {
                for tg in target_groups {
                    if let Some(tg_name) = &tg.target_group_name {
                        results.push(ElbResourceAddress::TargetGroup(region_name.clone(), tg_name.clone()).to_path_buf());
                    }
                }
            }
        }

        Ok(results)
    }
}
