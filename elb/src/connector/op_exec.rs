use std::{collections::HashMap, path::Path, str::FromStr};

use anyhow::{Context, bail};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error_util::invalid_op,
    op_exec_output,
};
use aws_sdk_elasticloadbalancingv2::types::{
    Action as AwsAction, ActionTypeEnum, Certificate as AwsCertificate, FixedResponseActionConfig, ForwardActionConfig,
    IpAddressType, LoadBalancerSchemeEnum, LoadBalancerTypeEnum, Matcher, ProtocolEnum, RedirectActionConfig,
    TargetDescription, TargetGroupAttribute, TargetTypeEnum,
};

use crate::{addr::ElbResourceAddress, op::ElbConnectorOp, tags::tag_diff};

use super::ElbConnector;

impl ElbConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = ElbResourceAddress::from_path(addr)?;
        let op = ElbConnectorOp::from_str(op)?;

        let account_id = self.account_id.lock().await.clone();
        if account_id.is_empty() {
            bail!("No account ID available");
        }

        match &addr {
            ElbResourceAddress::LoadBalancer(region, lb_name) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    ElbConnectorOp::CreateLoadBalancer(lb) => {
                        let mut request = client
                            .create_load_balancer()
                            .name(lb_name)
                            .set_subnets(Some(lb.subnets.clone()))
                            .set_security_groups(Some(lb.security_groups.clone()))
                            .scheme(LoadBalancerSchemeEnum::from_str(&lb.scheme)?)
                            .r#type(LoadBalancerTypeEnum::from_str(&lb.load_balancer_type)?)
                            .ip_address_type(IpAddressType::from_str(&lb.ip_address_type)?);

                        request = request.set_tags(lb.tags.into());

                        let response = request.send().await?;

                        let lb_arn = response
                            .load_balancers()
                            .first()
                            .and_then(|lb| lb.load_balancer_arn().map(|s| s.to_string()))
                            .context("Failed to get load balancer ARN from response")?;

                        let dns_name = response
                            .load_balancers()
                            .first()
                            .and_then(|lb| lb.dns_name().map(|s| s.to_string()))
                            .context("Failed to get DNS name from response")?;

                        op_exec_output!(
                            Some([
                                ("load_balancer_arn", Some(lb_arn)),
                                ("dns_name", Some(dns_name)),
                                ("load_balancer_name", Some(lb_name.clone()))
                            ]),
                            format!("Created load balancer `{}`", lb_name)
                        )
                    }
                    ElbConnectorOp::UpdateLoadBalancerTags(old_tags, new_tags) => {
                        // First get the load balancer ARN
                        let response = client.describe_load_balancers().names(lb_name.clone()).send().await?;

                        let lb_arn = response
                            .load_balancers()
                            .first()
                            .and_then(|lb| lb.load_balancer_arn())
                            .context("Load balancer not found")?;

                        let (remove_keys, add_tags) = tag_diff(&old_tags, &new_tags)?;

                        if !remove_keys.is_empty() {
                            client
                                .remove_tags()
                                .resource_arns(lb_arn)
                                .set_tag_keys(Some(remove_keys))
                                .send()
                                .await?;
                        }

                        if !add_tags.is_empty() {
                            client
                                .add_tags()
                                .resource_arns(lb_arn)
                                .set_tags(Some(add_tags))
                                .send()
                                .await?;
                        }

                        op_exec_output!(format!("Updated tags for load balancer `{}`", lb_name))
                    }
                    ElbConnectorOp::DeleteLoadBalancer => {
                        let response = client.describe_load_balancers().names(lb_name.clone()).send().await?;

                        let lb_arn = response
                            .load_balancers()
                            .first()
                            .and_then(|lb| lb.load_balancer_arn())
                            .context("Load balancer not found")?;

                        client.delete_load_balancer().load_balancer_arn(lb_arn).send().await?;

                        op_exec_output!(format!("Deleted load balancer `{}`", lb_name))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ElbResourceAddress::TargetGroup(region, tg_name) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    ElbConnectorOp::CreateTargetGroup(tg) => {
                        let mut request = client
                            .create_target_group()
                            .name(tg_name)
                            .protocol(ProtocolEnum::from_str(&tg.protocol)?)
                            .port(tg.port)
                            .vpc_id(tg.vpc_id.clone())
                            .target_type(TargetTypeEnum::from_str(&tg.target_type)?);

                        if let Some(ref health_check) = tg.health_check {
                            request = request
                                .health_check_enabled(health_check.enabled)
                                .health_check_interval_seconds(health_check.interval_seconds)
                                .health_check_path(health_check.path.clone())
                                .health_check_port(health_check.port.clone())
                                .health_check_protocol(ProtocolEnum::from_str(&health_check.protocol)?)
                                .health_check_timeout_seconds(health_check.timeout_seconds)
                                .healthy_threshold_count(health_check.healthy_threshold_count)
                                .unhealthy_threshold_count(health_check.unhealthy_threshold_count);
                        }

                        request = request.set_tags(tg.tags.into());

                        let response = request.send().await?;

                        let tg_arn = response
                            .target_groups()
                            .first()
                            .and_then(|tg| tg.target_group_arn().map(|s| s.to_string()))
                            .context("Failed to get target group ARN from response")?;

                        op_exec_output!(
                            Some([
                                ("target_group_arn", Some(tg_arn)),
                                ("target_group_name", Some(tg_name.clone()))
                            ]),
                            format!("Created target group `{}`", tg_name)
                        )
                    }
                    ElbConnectorOp::DeleteTargetGroup => {
                        let response = client.describe_target_groups().names(tg_name.clone()).send().await?;

                        let tg_arn = response
                            .target_groups()
                            .first()
                            .and_then(|tg| tg.target_group_arn())
                            .context("Target group not found")?;

                        client.delete_target_group().target_group_arn(tg_arn).send().await?;

                        op_exec_output!(format!("Deleted target group `{}`", tg_name))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
            ElbResourceAddress::Listener(region, lb_name, listener_id) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    ElbConnectorOp::CreateListener(listener) => {
                        // First get the load balancer ARN
                        let lb_response = client.describe_load_balancers().names(lb_name.clone()).send().await?;

                        let lb_arn = lb_response
                            .load_balancers()
                            .first()
                            .and_then(|lb| lb.load_balancer_arn())
                            .context("Load balancer not found")?;

                        let mut request = client
                            .create_listener()
                            .load_balancer_arn(lb_arn)
                            .port(listener.port)
                            .protocol(ProtocolEnum::from_str(&listener.protocol)?)
                            .set_ssl_policy(listener.ssl_policy.clone());

                        // Convert actions
                        if !listener.default_actions.is_empty() {
                            let aws_actions: Vec<AwsAction> = listener
                                .default_actions
                                .iter()
                                .map(|action| {
                                    let mut aws_action = AwsAction::builder().r#type(match action.action_type.as_str() {
                                        "forward" => ActionTypeEnum::Forward,
                                        "redirect" => ActionTypeEnum::Redirect,
                                        "fixed-response" => ActionTypeEnum::FixedResponse,
                                        _ => ActionTypeEnum::Forward,
                                    });

                                    if let Some(ref target_group_arn) = action.target_group_arn {
                                        aws_action = aws_action.target_group_arn(target_group_arn);
                                    }

                                    aws_action.build()
                                })
                                .collect();
                            request = request.set_default_actions(Some(aws_actions));
                        }

                        request = request.set_tags(listener.tags.into());

                        let response = request.send().await?;

                        let listener_arn = response
                            .listeners()
                            .first()
                            .and_then(|listener| listener.listener_arn().map(|s| s.to_string()))
                            .context("Failed to get listener ARN from response")?;

                        op_exec_output!(
                            Some([
                                ("listener_arn", Some(listener_arn)),
                                ("listener_id", Some(listener_id.clone()))
                            ]),
                            format!("Created listener `{}` for load balancer `{}`", listener_id, lb_name)
                        )
                    }
                    ElbConnectorOp::DeleteListener => {
                        // In a real implementation, you'd need to store and retrieve the listener ARN
                        // For now, we'll use a placeholder approach
                        op_exec_output!(format!(
                            "Would delete listener `{}` for load balancer `{}`",
                            listener_id, lb_name
                        ))
                    }
                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
