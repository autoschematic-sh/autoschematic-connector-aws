use autoschematic_core::{
    connector::{Resource, ResourceAddress},
    util::RON,
};
use serde::{Deserialize, Serialize};

use super::{addr::ElbResourceAddress, tags::Tags};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct HealthCheck {
    pub enabled: bool,
    pub protocol: String,
    pub port: String,
    pub path: String,
    pub interval_seconds: i32,
    pub timeout_seconds: i32,
    pub healthy_threshold_count: i32,
    pub unhealthy_threshold_count: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LoadBalancer {
    pub name: String,
    pub load_balancer_type: String, // application, network, or gateway
    pub scheme: String,             // internet-facing or internal
    pub vpc_id: String,
    pub security_groups: Vec<String>,
    pub subnets: Vec<String>,
    pub ip_address_type: String, // ipv4 or dualstack
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TargetGroup {
    pub name: String,
    pub protocol: String,
    pub port: i32,
    pub vpc_id: String,
    pub target_type: String, // instance, ip, lambda
    pub health_check: Option<HealthCheck>,
    pub targets: Vec<String>,
    pub tags: Tags,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Certificate {
    pub certificate_arn: String,
    pub is_default:      bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Action {
    pub action_type: String, // forward, redirect, fixed-response
    pub target_group_arn: Option<String>,
    pub redirect_config: Option<RedirectConfig>,
    pub fixed_response_config: Option<FixedResponseConfig>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct RedirectConfig {
    pub host: Option<String>,
    pub path: Option<String>,
    pub port: Option<String>,
    pub protocol: Option<String>,
    pub query: Option<String>,
    pub status_code: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct FixedResponseConfig {
    pub status_code:  Option<String>,
    pub content_type: Option<String>,
    pub message_body: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Listener {
    pub load_balancer_arn: String,
    pub port: i32,
    pub protocol: String,
    pub ssl_policy: Option<String>,
    pub certificates: Option<Vec<Certificate>>,
    pub default_actions: Vec<Action>,
    pub tags: Tags,
}

pub enum ElbResource {
    LoadBalancer(LoadBalancer),
    TargetGroup(TargetGroup),
    Listener(Listener),
}

impl Resource for ElbResource {
    fn to_bytes(&self) -> Result<Vec<u8>, anyhow::Error> {
        let pretty_config = autoschematic_core::util::PrettyConfig::default().struct_names(true);

        match self {
            ElbResource::LoadBalancer(lb) => match RON.to_string_pretty(&lb, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            ElbResource::TargetGroup(tg) => match RON.to_string_pretty(&tg, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
            ElbResource::Listener(listener) => match RON.to_string_pretty(&listener, pretty_config) {
                Ok(s) => Ok(s.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    fn from_bytes(addr: &impl ResourceAddress, s: &[u8]) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        let addr = ElbResourceAddress::from_path(&addr.to_path_buf())?;

        let s = str::from_utf8(s)?;

        match addr {
            ElbResourceAddress::LoadBalancer(_region, _name) => Ok(ElbResource::LoadBalancer(RON.from_str(s)?)),
            ElbResourceAddress::TargetGroup(_region, _name) => Ok(ElbResource::TargetGroup(RON.from_str(s)?)),
            ElbResourceAddress::Listener(_region, _lb_name, _listener_id) => Ok(ElbResource::Listener(RON.from_str(s)?)),
        }
    }
}
