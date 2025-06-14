pub use crate::addr::ElbResourceAddress;
pub use crate::resource::ElbResource;
use crate::resource::{self, FixedResponseConfig, RedirectConfig};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::config::ElbConnectorConfig;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterOutput, GetResourceOutput, OpExecOutput, OpPlanOutput, Resource, ResourceAddress,
    },
    diag::DiagnosticOutput,
    util::{ron_check_eq, ron_check_syntax},
};
use aws_config::{BehaviorVersion, Region, meta::region::RegionProviderChain, timeout::TimeoutConfig};
use tokio::sync::Mutex;

use autoschematic_connector_aws_core::config::AwsServiceConfig;

#[derive(Default)]
pub struct ElbConnector {
    client_cache: Mutex<HashMap<String, Arc<aws_sdk_elasticloadbalancingv2::Client>>>,
    account_id: Mutex<String>,
    config: Mutex<ElbConnectorConfig>,
    prefix: PathBuf,
}

impl ElbConnector {
    async fn get_or_init_client(&self, region_s: &str) -> anyhow::Result<Arc<aws_sdk_elasticloadbalancingv2::Client>> {
        let mut cache = self.client_cache.lock().await;

        if !cache.contains_key(region_s) {
            let region = RegionProviderChain::first_try(Region::new(region_s.to_owned()));

            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .timeout_config(
                    TimeoutConfig::builder()
                        .connect_timeout(Duration::from_secs(30))
                        .operation_timeout(Duration::from_secs(30))
                        .operation_attempt_timeout(Duration::from_secs(30))
                        .read_timeout(Duration::from_secs(30))
                        .build(),
                )
                .load()
                .await;
            let client = aws_sdk_elasticloadbalancingv2::Client::new(&config);
            cache.insert(region_s.to_string(), Arc::new(client));
        }

        let Some(client) = cache.get(region_s) else {
            bail!("Failed to get client for region {}", region_s);
        };

        Ok(client.clone())
    }
}

#[async_trait]
impl Connector for ElbConnector {
    async fn filter(&self, addr: &Path) -> Result<FilterOutput, anyhow::Error> {
        if let Ok(_addr) = ElbResourceAddress::from_path(addr) {
            Ok(FilterOutput::Resource)
        } else {
            Ok(FilterOutput::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Box<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Box::new(ElbConnector {
            prefix: prefix.into(),
            ..Default::default()
        }))
    }

    async fn init(&self) -> anyhow::Result<()> {
        let elb_config: ElbConnectorConfig = ElbConnectorConfig::try_load(&self.prefix).await?;

        let account_id = elb_config.verify_sts().await?;

        *self.client_cache.lock().await = HashMap::new();
        *self.config.lock().await = elb_config;
        *self.account_id.lock().await = account_id;
        Ok(())
    }

    async fn list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
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

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(region, load_balancer_name) => {
                let client = self.get_or_init_client(&region).await?;

                // Find the specific load balancer
                let load_balancers_resp = client.describe_load_balancers().names(&load_balancer_name).send().await?;

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

                // Construct the LoadBalancer resource
                let lb_resource = resource::LoadBalancer {
                    name: load_balancer_name,
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

                // Find the specific target group
                let target_groups_resp = client.describe_target_groups().names(&target_group_name).send().await?;

                let Some(target_groups) = target_groups_resp.target_groups else {
                    return Ok(None);
                };

                if target_groups.is_empty() {
                    return Ok(None);
                }

                let tg = &target_groups[0];

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
                let health_check = match &tg.health_check_protocol {
                    Some(protocol) => resource::HealthCheck {
                        protocol: protocol.as_str().to_string(),
                        port: tg.health_check_port.clone().unwrap_or_else(|| "traffic-port".to_string()),
                        path: tg.health_check_path.clone(),
                        interval_seconds: tg.health_check_interval_seconds.unwrap_or(30),
                        timeout_seconds: tg.health_check_timeout_seconds.unwrap_or(5),
                        healthy_threshold_count: tg.healthy_threshold_count.unwrap_or(5),
                        unhealthy_threshold_count: tg.unhealthy_threshold_count.unwrap_or(2),
                    },
                    None => {
                        // Default health check
                        resource::HealthCheck {
                            protocol: "HTTP".to_string(),
                            port: "traffic-port".to_string(),
                            path: Some("/".to_string()),
                            interval_seconds: 30,
                            timeout_seconds: 5,
                            healthy_threshold_count: 5,
                            unhealthy_threshold_count: 2,
                        }
                    }
                };

                // Construct the TargetGroup resource
                let tg_resource = resource::TargetGroup {
                    name: target_group_name,
                    protocol: tg.protocol().map_or_else(|| "HTTP".to_string(), |p| p.as_str().to_string()),
                    port: tg.port.unwrap_or(80),
                    vpc_id: tg.vpc_id.clone().unwrap_or_default(),
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
                let load_balancers_resp = client.describe_load_balancers().names(&load_balancer_name).send().await?;

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
                let listener_arn = format!("{}/listener/{}", lb_arn, listener_id);

                // Find the specific listener
                let listeners_resp = client.describe_listeners().listener_arns(&listener_arn).send().await?;

                let Some(listeners) = listeners_resp.listeners else {
                    return Ok(None);
                };

                if listeners.is_empty() {
                    return Ok(None);
                }

                let listener = &listeners[0];

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
                                a.fixed_response_config.as_ref().map(|fixed_response_config| FixedResponseConfig {
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

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!()
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!()
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(_, _) => ron_check_eq::<resource::LoadBalancer>(a, b),
            ElbResourceAddress::TargetGroup(_, _) => ron_check_eq::<resource::TargetGroup>(a, b),
            ElbResourceAddress::Listener(_, _, _) => ron_check_eq::<resource::Listener>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<DiagnosticOutput, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(_, _) => ron_check_syntax::<resource::LoadBalancer>(a),
            ElbResourceAddress::TargetGroup(_, _) => ron_check_syntax::<resource::TargetGroup>(a),
            ElbResourceAddress::Listener(_, _, _) => ron_check_syntax::<resource::Listener>(a),
        }
    }
}
