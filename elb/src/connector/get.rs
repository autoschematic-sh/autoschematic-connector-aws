use std::path::Path;

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};

use crate::{
    addr::ElbResourceAddress,
    resource::{self, ElbResource, FixedResponseConfig, RedirectConfig},
};

use super::ElbConnector;

impl ElbConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(region, load_balancer_name) => {
                let client = self.get_or_init_client(&region).await?;

                let Ok(load_balancers_resp) = client.describe_load_balancers().names(&load_balancer_name).send().await else {
                    return Ok(None);
                };

                let Some(load_balancers) = load_balancers_resp.load_balancers else {
                    return Ok(None);
                };

                if load_balancers.is_empty() {
                    return Ok(None);
                }

                let lb = &load_balancers[0];

                // Get tags for this load balancer
                let tags = if let Some(lb_arn) = &lb.load_balancer_arn {
                    let tags_resp = client.describe_tags().resource_arns(lb_arn).send().await?;

                    if let Some(tag_descriptions) = tags_resp.tag_descriptions {
                        if !tag_descriptions.is_empty() && tag_descriptions[0].tags.is_some() {
                            tag_descriptions[0].tags.clone().into()
                        } else {
                            Default::default()
                        }
                    } else {
                        Default::default()
                    }
                } else {
                    Default::default()
                };

                let lb_resource = resource::LoadBalancer {
                    load_balancer_type: lb
                        .r#type
                        .as_ref()
                        .map_or_else(|| "application".to_string(), |t| t.as_str().to_string()),
                    scheme: lb
                        .scheme
                        .as_ref()
                        .map_or_else(|| "internet-facing".to_string(), |s| s.as_str().to_string()),
                    vpc_id: lb.vpc_id.clone().unwrap_or_default(),
                    security_groups: lb.security_groups.clone().unwrap_or_default(),
                    subnets: lb
                        .availability_zones
                        .as_ref()
                        .map_or_else(Vec::new, |azs| azs.iter().filter_map(|az| az.subnet_id.clone()).collect()),
                    ip_address_type: lb
                        .ip_address_type
                        .as_ref()
                        .map_or_else(|| "ipv4".to_string(), |t| t.as_str().to_string()),
                    tags,
                };

                Ok(Some(GetResourceOutput {
                    resource_definition: ElbResource::LoadBalancer(lb_resource).to_bytes()?,
                    outputs: None,
                }))
            }
            ElbResourceAddress::TargetGroup(region, target_group_name) => {
                let client = self.get_or_init_client(&region).await?;
                

                // TODO can target groups have the same name with a different ARN? I sure as hell think they can!
                let Ok(target_groups_resp) = client.describe_target_groups().names(&target_group_name).send().await else {
                    return Ok(None);
                };

                let Some(target_groups) = target_groups_resp.target_groups else {
                    return Ok(None);
                };

                let Some(tg) = target_groups.first() else {
                    return Ok(None);
                };

                // Get tags for this target group
                let tags = if let Some(tg_arn) = &tg.target_group_arn {
                    let tags_resp = client.describe_tags().resource_arns(tg_arn).send().await?;

                    if let Some(tag_descriptions) = tags_resp.tag_descriptions {
                        if !tag_descriptions.is_empty() && tag_descriptions[0].tags.is_some() {
                            tag_descriptions[0].tags.clone().into()
                        } else {
                            Default::default()
                        }
                    } else {
                        Default::default()
                    }
                } else {
                    Default::default()
                };

                // Get the registered targets
                let registered_targets = if let Some(tg_arn) = &tg.target_group_arn {
                    let targets_resp = client.describe_target_health().target_group_arn(tg_arn).send().await?;

                    targets_resp.target_health_descriptions.map_or_else(Vec::new, |descriptions| {
                        descriptions
                            .iter()
                            .filter_map(|desc| desc.target.as_ref().and_then(|target| target.id.clone()))
                            .collect()
                    })
                } else {
                    Vec::new()
                };

                // Construct health check
                let health_check = tg.health_check_protocol.as_ref().map(|protocol| resource::HealthCheck {
                        enabled: tg.health_check_enabled.unwrap_or(false),
                        protocol: protocol.as_str().to_string(),
                        port: tg.health_check_port.clone().unwrap_or("traffic-port".to_string()),
                        path: tg.health_check_path.clone().unwrap_or("/".to_string()),
                        interval_seconds: tg.health_check_interval_seconds.unwrap_or(30),
                        timeout_seconds: tg.health_check_timeout_seconds.unwrap_or(5),
                        healthy_threshold_count: tg.healthy_threshold_count.unwrap_or(5),
                        unhealthy_threshold_count: tg.unhealthy_threshold_count.unwrap_or(2),
                    });

                // Construct the TargetGroup resource
                let tg_resource = resource::TargetGroup {
                    protocol: tg.protocol().map_or_else(|| "HTTP".to_string(), |p| p.as_str().to_string()),
                    port: tg.port,
                    vpc_id: tg.vpc_id.clone(),
                    target_type: tg
                        .target_type()
                        .map_or_else(|| "instance".to_string(), |t| t.as_str().to_string()),
                    health_check,
                    targets: registered_targets,
                    tags,
                };

                Ok(Some(GetResourceOutput {
                    resource_definition: ElbResource::TargetGroup(tg_resource).to_bytes()?,
                    outputs: None,
                }))
            }
            ElbResourceAddress::Listener(region, load_balancer_name, listener_id) => {
                let client = self.get_or_init_client(&region).await?;

                // First, get the load balancer ARN
                let Ok(load_balancers_resp) = client.describe_load_balancers().names(&load_balancer_name).send().await else {
                    return Ok(None);
                };

                let Some(load_balancers) = load_balancers_resp.load_balancers else {
                    return Ok(None);
                };

                if load_balancers.is_empty() {
                    return Ok(None);
                }

                let Some(lb_arn) = &load_balancers[0].load_balancer_arn else {
                    return Ok(None);
                };

                // Now, reconstruct the full listener ARN
                let listener_arn = format!("{lb_arn}/listener/{listener_id}");

                // Find the specific listener
                let Ok(listeners_resp) = client.describe_listeners().listener_arns(&listener_arn).send().await else {
                    return Ok(None);
                };

                let Some(listeners) = listeners_resp.listeners else {
                    return Ok(None);
                };

                if listeners.is_empty() {
                    return Ok(None);
                }

                let Some(listener) = listeners.first() else {
                    return Ok(None);
                };

                // Get tags for this listener
                let tags = if let Some(listener_arn) = &listener.listener_arn {
                    let tags_resp = client.describe_tags().resource_arns(listener_arn).send().await?;

                    if let Some(tag_descriptions) = tags_resp.tag_descriptions {
                        if !tag_descriptions.is_empty() && tag_descriptions[0].tags.is_some() {
                            tag_descriptions[0].tags.clone().into()
                        } else {
                            Default::default()
                        }
                    } else {
                        Default::default()
                    }
                } else {
                    Default::default()
                };

                // Convert certificates
                let certificates = listener.certificates.as_ref().map(|certs| {
                    certs
                        .iter()
                        .map(|c| resource::Certificate {
                            certificate_arn: c.certificate_arn.clone().unwrap_or_default(),
                            is_default:      c.is_default.unwrap_or(false),
                        })
                        .collect()
                });

                // Convert actions
                let default_actions = listener.default_actions.as_ref().map_or_else(Vec::new, |actions| {
                    actions
                        .iter()
                        .map(|a| {
                            let action_type = a.r#type().map_or_else(|| "forward".to_string(), |t| t.as_str().to_string());

                            let target_group_arn = if action_type == "forward" {
                                a.target_group_arn.clone()
                            } else {
                                None
                            };

                            let redirect_config = if action_type == "redirect" {
                                a.redirect_config.as_ref().map(|redirect_config| RedirectConfig {
                                    host: redirect_config.host.clone(),
                                    path: redirect_config.path.clone(),
                                    port: redirect_config.port.clone(),
                                    protocol: redirect_config.protocol.clone(),
                                    query: redirect_config.query.clone(),
                                    status_code: redirect_config.status_code.as_ref().map(|s| s.to_string()),
                                })
                            } else {
                                None
                            };

                            let fixed_response_config = if action_type == "fixed-response" {
                                a.fixed_response_config
                                    .as_ref()
                                    .map(|fixed_response_config| FixedResponseConfig {
                                        status_code:  fixed_response_config.status_code.as_ref().map(|s| s.to_string()),
                                        content_type: fixed_response_config.content_type.clone(),
                                        message_body: fixed_response_config.message_body.clone(),
                                    })
                            } else {
                                None
                            };

                            resource::Action {
                                action_type,
                                target_group_arn,
                                redirect_config,
                                fixed_response_config,
                            }
                        })
                        .collect()
                });

                // Construct the Listener resource
                let listener_resource = resource::Listener {
                    load_balancer_arn: lb_arn.clone(),
                    port: listener.port.unwrap_or(80),
                    protocol: listener
                        .protocol()
                        .map_or_else(|| "HTTP".to_string(), |p| p.as_str().to_string()),
                    ssl_policy: listener.ssl_policy.clone(),
                    certificates,
                    default_actions,
                    tags,
                };

                Ok(Some(GetResourceOutput {
                    resource_definition: ElbResource::Listener(listener_resource).to_bytes()?,
                    outputs: None,
                }))
            }
        }
    }
}
