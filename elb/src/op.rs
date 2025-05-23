use autoschematic_core::{connector::ConnectorOp, util::RON};
use serde::{Deserialize, Serialize};

use super::{
    resource::{Action, Certificate, HealthCheck, Listener, LoadBalancer, TargetGroup},
    tags::Tags,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum ElbConnectorOp {
    // Load Balancer operations
    CreateLoadBalancer(LoadBalancer),
    UpdateLoadBalancerTags(Tags, Tags),
    AddSecurityGroups {
        security_group_ids: Vec<String>,
    },
    RemoveSecurityGroups {
        security_group_ids: Vec<String>,
    },
    UpdateIpAddressType {
        ip_address_type: String, // ipv4 or dualstack
    },
    UpdateSubnets {
        subnets: Vec<String>,
    },
    DeleteLoadBalancer,

    // Target Group operations
    CreateTargetGroup(TargetGroup),
    UpdateTargetGroupTags(Tags, Tags),
    UpdateTargetGroupAttributes {
        deregistration_delay_seconds: Option<i32>,
        slow_start_seconds: Option<i32>,
        stickiness_enabled: Option<bool>,
        stickiness_type: Option<String>,
        stickiness_cookie_duration_seconds: Option<i32>,
    },
    UpdateHealthCheck(HealthCheck),
    RegisterTargets {
        targets: Vec<String>,
    },
    DeregisterTargets {
        targets: Vec<String>,
    },
    DeleteTargetGroup,

    // Listener operations
    CreateListener(Listener),
    UpdateListenerTags(Tags, Tags),
    ModifyListener {
        port: Option<i32>,
        protocol: Option<String>,
        ssl_policy: Option<String>,
        default_actions: Option<Vec<Action>>,
    },
    AddCertificates {
        certificates: Vec<Certificate>,
    },
    RemoveCertificates {
        certificate_arns: Vec<String>,
    },
    DeleteListener,

    // Listener Rule operations
    CreateRule {
        listener_arn: String,
        priority: i32,
        conditions: Vec<ron::Value>, // Complex structure, using ron::Value
        actions: Vec<Action>,
        tags: Tags,
    },
    ModifyRule {
        rule_arn: String,
        conditions: Option<Vec<ron::Value>>,
        actions: Option<Vec<Action>>,
    },
    UpdateRuleTags {
        rule_arn: String,
        tags_to_add: Tags,
        tags_to_remove: Tags,
    },
    DeleteRule {
        rule_arn: String,
    },
}

impl ConnectorOp for ElbConnectorOp {
    fn to_string(&self) -> Result<String, anyhow::Error> {
        Ok(RON.to_string(self)?)
    }

    fn from_str(s: &str) -> Result<Self, anyhow::Error>
    where
        Self: Sized,
    {
        Ok(RON.from_str(s)?)
    }
}
