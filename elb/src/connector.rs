pub use crate::addr::ElbResourceAddress;
pub use crate::resource::ElbResource;
use crate::resource::{self, Action, Certificate, HealthCheck, Listener, LoadBalancer, TargetGroup};
use crate::tags::Tags;

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

mod get;
mod list;
mod op_exec;
mod plan;

use crate::config::ElbConnectorConfig;
use anyhow::bail;
use async_trait::async_trait;
use autoschematic_core::{
    connector::{
        Connector, ConnectorOutbox, FilterResponse, GetResourceResponse, OpExecResponse, PlanResponseElement, Resource, ResourceAddress,
        SkeletonResponse,
    },
    diag::DiagnosticResponse,
    skeleton,
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
    async fn filter(&self, addr: &Path) -> Result<FilterResponse, anyhow::Error> {
        if let Ok(_addr) = ElbResourceAddress::from_path(addr) {
            Ok(FilterResponse::Resource)
        } else {
            Ok(FilterResponse::None)
        }
    }

    async fn new(_name: &str, prefix: &Path, _outbox: ConnectorOutbox) -> Result<Arc<dyn Connector>, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(Arc::new(ElbConnector {
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

    async fn list(&self, subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        self.do_list(subpath).await
    }

    async fn get(&self, addr: &Path) -> Result<Option<GetResourceResponse>, anyhow::Error> {
        self.do_get(addr).await
    }

    async fn plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<PlanResponseElement>, anyhow::Error> {
        self.do_plan(addr, current, desired).await
    }

    async fn op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        self.do_op_exec(addr, op).await
    }

    async fn get_skeletons(&self) -> Result<Vec<SkeletonResponse>, anyhow::Error> {
        let mut res = Vec::new();

        let region = String::from("[region]");
        let lb_name = String::from("[load_balancer_name]");

        // Application Load Balancer
        res.push(skeleton!(
            ElbResourceAddress::LoadBalancer(region.clone(), lb_name.clone()),
            ElbResource::LoadBalancer(LoadBalancer {
                load_balancer_type: String::from("application"),
                scheme: String::from("internet-facing"),
                vpc_id: String::from("[vpc_id]"),
                security_groups: vec![String::from("[security_group_id]")],
                subnets: vec![String::from("[subnet_id_1]"), String::from("[subnet_id_2]")],
                ip_address_type: String::from("ipv4"),
                tags: Tags::default(),
            })
        ));

        // Target Group
        let tg_name = String::from("[target_group_name]");
        res.push(skeleton!(
            ElbResourceAddress::TargetGroup(region.clone(), tg_name),
            ElbResource::TargetGroup(TargetGroup {
                protocol: String::from("HTTP"),
                port: Some(80),
                vpc_id: Some(String::from("[vpc_id]")),
                target_type: String::from("instance"),
                health_check: Some(HealthCheck {
                    enabled: true,
                    protocol: String::from("HTTP"),
                    port: String::from("traffic-port"),
                    path: String::from("/health"),
                    interval_seconds: 30,
                    timeout_seconds: 5,
                    healthy_threshold_count: 2,
                    unhealthy_threshold_count: 5,
                }),
                targets: vec![String::from("[instance_id]")],
                tags: Tags::default(),
            })
        ));

        // HTTPS Listener
        let listener_id = String::from("[listener_id]");
        res.push(skeleton!(
            ElbResourceAddress::Listener(region.clone(), lb_name.clone(), listener_id),
            ElbResource::Listener(Listener {
                load_balancer_arn: String::from("[load_balancer_arn]"),
                port: 443,
                protocol: String::from("HTTPS"),
                ssl_policy: Some(String::from("ELBSecurityPolicy-TLS-1-2-2017-01")),
                certificates: Some(vec![Certificate {
                    certificate_arn: String::from("[certificate_arn]"),
                    is_default:      true,
                }]),
                default_actions: vec![Action {
                    action_type: String::from("forward"),
                    target_group_arn: Some(String::from("[target_group_arn]")),
                    redirect_config: None,
                    fixed_response_config: None,
                }],
                tags: Tags::default(),
            })
        ));

        // Network Load Balancer
        let nlb_name = String::from("[network_load_balancer_name]");
        res.push(skeleton!(
            ElbResourceAddress::LoadBalancer(region.clone(), nlb_name.clone()),
            ElbResource::LoadBalancer(LoadBalancer {
                load_balancer_type: String::from("network"),
                scheme: String::from("internal"),
                vpc_id: String::from("[vpc_id]"),
                security_groups: vec![],
                subnets: vec![String::from("[subnet_id_1]"), String::from("[subnet_id_2]")],
                ip_address_type: String::from("ipv4"),
                tags: Tags::default(),
            })
        ));

        // TCP Target Group for Network Load Balancer
        let tcp_tg_name = String::from("[tcp_target_group_name]");
        res.push(skeleton!(
            ElbResourceAddress::TargetGroup(region.clone(), tcp_tg_name),
            ElbResource::TargetGroup(TargetGroup {
                protocol: String::from("TCP"),
                port: Some(80),
                vpc_id: Some(String::from("[vpc_id]")),
                target_type: String::from("ip"),
                health_check: Some(HealthCheck {
                    enabled: true,
                    protocol: String::from("TCP"),
                    port: String::from("traffic-port"),
                    path: String::from(""),
                    interval_seconds: 30,
                    timeout_seconds: 10,
                    healthy_threshold_count: 3,
                    unhealthy_threshold_count: 3,
                }),
                targets: vec![String::from("[ip_address]:80")],
                tags: Tags::default(),
            })
        ));

        // TCP Listener for Network Load Balancer
        let tcp_listener_id = String::from("[tcp_listener_id]");
        res.push(skeleton!(
            ElbResourceAddress::Listener(region.clone(), nlb_name, tcp_listener_id),
            ElbResource::Listener(Listener {
                load_balancer_arn: String::from("[network_load_balancer_arn]"),
                port: 80,
                protocol: String::from("TCP"),
                ssl_policy: None,
                certificates: None,
                default_actions: vec![Action {
                    action_type: String::from("forward"),
                    target_group_arn: Some(String::from("[tcp_target_group_arn]")),
                    redirect_config: None,
                    fixed_response_config: None,
                }],
                tags: Tags::default(),
            })
        ));

        Ok(res)
    }

    async fn eq(&self, addr: &Path, a: &[u8], b: &[u8]) -> anyhow::Result<bool> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(_, _) => ron_check_eq::<resource::LoadBalancer>(a, b),
            ElbResourceAddress::TargetGroup(_, _) => ron_check_eq::<resource::TargetGroup>(a, b),
            ElbResourceAddress::Listener(_, _, _) => ron_check_eq::<resource::Listener>(a, b),
        }
    }

    async fn diag(&self, addr: &Path, a: &[u8]) -> Result<Option<DiagnosticResponse>, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;

        match addr {
            ElbResourceAddress::LoadBalancer(_, _) => ron_check_syntax::<resource::LoadBalancer>(a),
            ElbResourceAddress::TargetGroup(_, _) => ron_check_syntax::<resource::TargetGroup>(a),
            ElbResourceAddress::Listener(_, _, _) => ron_check_syntax::<resource::Listener>(a),
        }
    }
}
