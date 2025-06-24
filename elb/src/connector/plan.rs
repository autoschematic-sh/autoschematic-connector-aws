use std::path::Path;

use autoschematic_core::{
    connector::{ConnectorOp, OpPlanOutput, ResourceAddress},
    connector_op,
    util::{RON, diff_ron_values, optional_string_from_utf8},
};

use crate::{
    addr::ElbResourceAddress,
    op::ElbConnectorOp,
    resource::{Listener, LoadBalancer, TargetGroup},
};

use super::ElbConnector;

impl ElbConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;
        let current = optional_string_from_utf8(current)?;
        let desired = optional_string_from_utf8(desired)?;

        match addr {
            ElbResourceAddress::LoadBalancer(region, lb_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_lb)) => {
                        let new_lb: LoadBalancer = RON.from_str(&new_lb)?;
                        Ok(vec![connector_op!(
                            ElbConnectorOp::CreateLoadBalancer(new_lb),
                            format!("Create new Load Balancer {}", lb_name)
                        )])
                    }
                    (Some(_old_lb), None) => Ok(vec![connector_op!(
                        ElbConnectorOp::DeleteLoadBalancer,
                        format!("DELETE Load Balancer {}", lb_name)
                    )]),
                    (Some(old_lb), Some(new_lb)) => {
                        let old_lb: LoadBalancer = RON.from_str(&old_lb)?;
                        let new_lb: LoadBalancer = RON.from_str(&new_lb)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_lb.tags != new_lb.tags {
                            let diff = diff_ron_values(&old_lb.tags, &new_lb.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                ElbConnectorOp::UpdateLoadBalancerTags(old_lb.tags, new_lb.tags),
                                format!("Modify tags for Load Balancer `{}`\n{}", lb_name, diff)
                            ));
                        }

                        // Check for security group changes
                        if old_lb.security_groups != new_lb.security_groups {
                            let old_sgs: std::collections::HashSet<_> = old_lb.security_groups.iter().collect();
                            let new_sgs: std::collections::HashSet<_> = new_lb.security_groups.iter().collect();

                            let to_remove: Vec<String> = old_sgs.difference(&new_sgs).map(|s| s.to_string()).collect();
                            let to_add: Vec<String> = new_sgs.difference(&old_sgs).map(|s| s.to_string()).collect();

                            if !to_remove.is_empty() {
                                ops.push(connector_op!(
                                    ElbConnectorOp::RemoveSecurityGroups {
                                        security_group_ids: to_remove,
                                    },
                                    format!("Remove security groups from Load Balancer `{}`", lb_name)
                                ));
                            }

                            if !to_add.is_empty() {
                                ops.push(connector_op!(
                                    ElbConnectorOp::AddSecurityGroups {
                                        security_group_ids: to_add,
                                    },
                                    format!("Add security groups to Load Balancer `{}`", lb_name)
                                ));
                            }
                        }

                        // Check for IP address type changes
                        if old_lb.ip_address_type != new_lb.ip_address_type {
                            ops.push(connector_op!(
                                ElbConnectorOp::UpdateIpAddressType {
                                    ip_address_type: new_lb.ip_address_type.clone(),
                                },
                                format!("Update IP address type for Load Balancer `{}`", lb_name)
                            ));
                        }

                        // Check for subnet changes
                        if old_lb.subnets != new_lb.subnets {
                            ops.push(connector_op!(
                                ElbConnectorOp::UpdateSubnets {
                                    subnets: new_lb.subnets.clone(),
                                },
                                format!("Update subnets for Load Balancer `{}`", lb_name)
                            ));
                        }

                        Ok(ops)
                    }
                }
            }
            ElbResourceAddress::TargetGroup(region, tg_name) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_tg)) => {
                        let new_tg: TargetGroup = RON.from_str(&new_tg)?;
                        let mut ops = vec![connector_op!(
                            ElbConnectorOp::CreateTargetGroup(new_tg.clone()),
                            format!("Create new Target Group {}", tg_name)
                        )];

                        // Register targets if any
                        if !new_tg.targets.is_empty() {
                            ops.push(connector_op!(
                                ElbConnectorOp::RegisterTargets {
                                    targets: new_tg.targets.clone(),
                                },
                                format!("Register targets with Target Group `{}`", tg_name)
                            ));
                        }

                        Ok(ops)
                    }
                    (Some(_old_tg), None) => Ok(vec![connector_op!(
                        ElbConnectorOp::DeleteTargetGroup,
                        format!("DELETE Target Group {}", tg_name)
                    )]),
                    (Some(old_tg), Some(new_tg)) => {
                        let old_tg: TargetGroup = RON.from_str(&old_tg)?;
                        let new_tg: TargetGroup = RON.from_str(&new_tg)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_tg.tags != new_tg.tags {
                            let diff = diff_ron_values(&old_tg.tags, &new_tg.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                ElbConnectorOp::UpdateTargetGroupTags(old_tg.tags, new_tg.tags),
                                format!("Modify tags for Target Group `{}`\n{}", tg_name, diff)
                            ));
                        }

                        // Check for health check changes
                        if old_tg.health_check != new_tg.health_check {
                            if let Some(new_health_check) = &new_tg.health_check {
                                ops.push(connector_op!(
                                    ElbConnectorOp::UpdateHealthCheck(new_health_check.clone()),
                                    format!("Update health check for Target Group `{}`", tg_name)
                                ));
                            }
                        }

                        // Check for target changes
                        if old_tg.targets != new_tg.targets {
                            let old_targets: std::collections::HashSet<_> = old_tg.targets.iter().collect();
                            let new_targets: std::collections::HashSet<_> = new_tg.targets.iter().collect();

                            let to_deregister: Vec<String> =
                                old_targets.difference(&new_targets).map(|s| s.to_string()).collect();
                            let to_register: Vec<String> =
                                new_targets.difference(&old_targets).map(|s| s.to_string()).collect();

                            if !to_deregister.is_empty() {
                                ops.push(connector_op!(
                                    ElbConnectorOp::DeregisterTargets { targets: to_deregister },
                                    format!("Deregister targets from Target Group `{}`", tg_name)
                                ));
                            }

                            if !to_register.is_empty() {
                                ops.push(connector_op!(
                                    ElbConnectorOp::RegisterTargets { targets: to_register },
                                    format!("Register targets with Target Group `{}`", tg_name)
                                ));
                            }
                        }

                        Ok(ops)
                    }
                }
            }
            ElbResourceAddress::Listener(region, lb_name, listener_id) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_listener)) => {
                        let new_listener: Listener = RON.from_str(&new_listener)?;
                        Ok(vec![connector_op!(
                            ElbConnectorOp::CreateListener(new_listener),
                            format!("Create new Listener {} for Load Balancer {}", listener_id, lb_name)
                        )])
                    }
                    (Some(_old_listener), None) => Ok(vec![connector_op!(
                        ElbConnectorOp::DeleteListener,
                        format!("DELETE Listener {} for Load Balancer {}", listener_id, lb_name)
                    )]),
                    (Some(old_listener), Some(new_listener)) => {
                        let old_listener: Listener = RON.from_str(&old_listener)?;
                        let new_listener: Listener = RON.from_str(&new_listener)?;
                        let mut ops = Vec::new();

                        // Check for tag changes
                        if old_listener.tags != new_listener.tags {
                            let diff = diff_ron_values(&old_listener.tags, &new_listener.tags).unwrap_or_default();
                            ops.push(connector_op!(
                                ElbConnectorOp::UpdateListenerTags(old_listener.tags, new_listener.tags),
                                format!("Modify tags for Listener `{}`\n{}", listener_id, diff)
                            ));
                        }

                        // Check for listener configuration changes
                        if old_listener.port != new_listener.port
                            || old_listener.protocol != new_listener.protocol
                            || old_listener.ssl_policy != new_listener.ssl_policy
                            || old_listener.default_actions != new_listener.default_actions
                        {
                            ops.push(connector_op!(
                                ElbConnectorOp::ModifyListener {
                                    port: if old_listener.port != new_listener.port {
                                        Some(new_listener.port)
                                    } else {
                                        None
                                    },
                                    protocol: if old_listener.protocol != new_listener.protocol {
                                        Some(new_listener.protocol.clone())
                                    } else {
                                        None
                                    },
                                    ssl_policy: if old_listener.ssl_policy != new_listener.ssl_policy {
                                        new_listener.ssl_policy.clone()
                                    } else {
                                        None
                                    },
                                    default_actions: if old_listener.default_actions != new_listener.default_actions {
                                        Some(new_listener.default_actions.clone())
                                    } else {
                                        None
                                    },
                                },
                                format!("Modify Listener `{}` configuration", listener_id)
                            ));
                        }

                        // Check for certificate changes
                        if old_listener.certificates != new_listener.certificates {
                            if let Some(new_certs) = &new_listener.certificates {
                                if let Some(old_certs) = &old_listener.certificates {
                                    let old_cert_arns: std::collections::HashSet<_> =
                                        old_certs.iter().map(|c| &c.certificate_arn).collect();
                                    let new_cert_arns: std::collections::HashSet<_> =
                                        new_certs.iter().map(|c| &c.certificate_arn).collect();

                                    let to_remove: Vec<String> =
                                        old_cert_arns.difference(&new_cert_arns).map(|s| s.to_string()).collect();
                                    let to_add: Vec<_> = new_certs
                                        .iter()
                                        .filter(|c| !old_cert_arns.contains(&c.certificate_arn))
                                        .cloned()
                                        .collect();

                                    if !to_remove.is_empty() {
                                        ops.push(connector_op!(
                                            ElbConnectorOp::RemoveCertificates {
                                                certificate_arns: to_remove,
                                            },
                                            format!("Remove certificates from Listener `{}`", listener_id)
                                        ));
                                    }

                                    if !to_add.is_empty() {
                                        ops.push(connector_op!(
                                            ElbConnectorOp::AddCertificates { certificates: to_add },
                                            format!("Add certificates to Listener `{}`", listener_id)
                                        ));
                                    }
                                } else {
                                    // No old certificates, add all new ones
                                    ops.push(connector_op!(
                                        ElbConnectorOp::AddCertificates {
                                            certificates: new_certs.clone(),
                                        },
                                        format!("Add certificates to Listener `{}`", listener_id)
                                    ));
                                }
                            } else if old_listener.certificates.is_some() {
                                // Remove all certificates
                                if let Some(old_certs) = &old_listener.certificates {
                                    let cert_arns: Vec<String> = old_certs.iter().map(|c| c.certificate_arn.clone()).collect();
                                    ops.push(connector_op!(
                                        ElbConnectorOp::RemoveCertificates {
                                            certificate_arns: cert_arns,
                                        },
                                        format!("Remove all certificates from Listener `{}`", listener_id)
                                    ));
                                }
                            }
                        }

                        Ok(ops)
                    }
                }
            }
        }
    }
}
