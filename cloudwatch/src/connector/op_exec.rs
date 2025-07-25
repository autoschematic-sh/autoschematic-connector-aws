use std::{collections::HashMap, path::Path};

use crate::tags::tag_diff;
use crate::{addr::CloudWatchResourceAddress, op::CloudWatchConnectorOp};
use anyhow::{Context, bail};
use autoschematic_core::{
    connector::{ConnectorOp, OpExecResponse, ResourceAddress},
    error_util::invalid_op,
    op_exec_output,
};

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecResponse, anyhow::Error> {
        let addr = CloudWatchResourceAddress::from_path(addr)?;
        let op = CloudWatchConnectorOp::from_str(op)?;
        let account_id = self.account_id.lock().await.clone();

        match &addr {
            CloudWatchResourceAddress::Alarm(region, alarm_name) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    CloudWatchConnectorOp::CreateAlarm(alarm) => {
                        let mut request = client.put_metric_alarm().alarm_name(alarm_name);

                        if let Some(description) = &alarm.description {
                            request = request.alarm_description(description);
                        }

                        if let Some(metric_name) = &alarm.metric_name {
                            request = request.metric_name(metric_name);
                        }

                        if let Some(namespace) = &alarm.namespace {
                            request = request.namespace(namespace);
                        }

                        if let Some(statistic) = &alarm.statistic {
                            request = request.statistic(aws_sdk_cloudwatch::types::Statistic::from(statistic.as_str()));
                        }

                        if let Some(dimensions) = &alarm.dimensions {
                            for dim in dimensions {
                                request = request.dimensions(
                                    aws_sdk_cloudwatch::types::Dimension::builder()
                                        .name(&dim.name)
                                        .value(&dim.value)
                                        .build(),
                                );
                            }
                        }

                        if let Some(period) = alarm.period {
                            request = request.period(period as i32);
                        }

                        request = request.evaluation_periods(alarm.evaluation_periods as i32);

                        if let Some(threshold) = alarm.threshold {
                            request = request.threshold(threshold);
                        }

                        request = request.comparison_operator(aws_sdk_cloudwatch::types::ComparisonOperator::from(
                            alarm.comparison_operator.as_str(),
                        ));

                        if let Some(treat_missing_data) = &alarm.treat_missing_data {
                            request = request.treat_missing_data(treat_missing_data);
                        }

                        if let Some(alarm_actions) = &alarm.alarm_actions {
                            for action in alarm_actions {
                                request = request.alarm_actions(&action.arn);
                            }
                        }

                        request.send().await?;

                        let arn = format!("arn:aws:cloudwatch:{region}:{account_id}:alarm:{alarm_name}");
                        op_exec_output!(
                            Some([("alarm_arn", Some(arn))]),
                            format!("Created CloudWatch alarm `{}`", alarm_name)
                        )
                    }

                    CloudWatchConnectorOp::DeleteAlarm => {
                        client.delete_alarms().alarm_names(alarm_name).send().await?;
                        op_exec_output!(format!("Deleted CloudWatch alarm `{}`", alarm_name))
                    }

                    CloudWatchConnectorOp::UpdateAlarm {
                        threshold,
                        evaluation_periods,
                        comparison_operator,
                        treat_missing_data,
                        ..
                    } => {
                        let mut request = client.put_metric_alarm().alarm_name(alarm_name);

                        if let Some(threshold) = threshold {
                            request = request.threshold(threshold);
                        }

                        if let Some(eval_periods) = evaluation_periods {
                            request = request.evaluation_periods(eval_periods as i32);
                        }

                        if let Some(operator) = comparison_operator {
                            request = request
                                .comparison_operator(aws_sdk_cloudwatch::types::ComparisonOperator::from(operator.as_str()));
                        }

                        if let Some(treat_missing) = treat_missing_data {
                            request = request.treat_missing_data(treat_missing);
                        }

                        request.send().await?;
                        op_exec_output!(format!("Updated CloudWatch alarm `{}`", alarm_name))
                    }

                    CloudWatchConnectorOp::UpdateAlarmTags(old_tags, new_tags) => {
                        let alarm_arn = format!("arn:aws:cloudwatch:{region}:{account_id}:alarm:{alarm_name}");
                        let (untag_keys, new_tagset) = tag_diff(&old_tags, &new_tags).context("Failed to generate tag diff")?;

                        if !untag_keys.is_empty() {
                            client
                                .untag_resource()
                                .resource_arn(&alarm_arn)
                                .set_tag_keys(Some(untag_keys))
                                .send()
                                .await
                                .context("Failed to remove tags")?;
                        }

                        if !new_tagset.is_empty() {
                            client
                                .tag_resource()
                                .resource_arn(&alarm_arn)
                                .set_tags(Some(new_tagset.into_iter().map(|t| t).collect()))
                                .send()
                                .await
                                .context("Failed to write new tags")?;
                        }

                        op_exec_output!(format!("Updated tags for CloudWatch alarm `{}`", alarm_name))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudWatchResourceAddress::Dashboard(region, dashboard_name) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    CloudWatchConnectorOp::CreateDashboard(dashboard) => {
                        let dashboard_body =
                            serde_json::to_string(&dashboard.body).context("Failed to serialize dashboard body")?;

                        client
                            .put_dashboard()
                            .dashboard_name(dashboard_name)
                            .dashboard_body(dashboard_body)
                            .send()
                            .await?;

                        let arn = format!("arn:aws:cloudwatch:{region}:{account_id}:dashboard/{dashboard_name}");
                        op_exec_output!(
                            Some([("dashboard_arn", Some(arn))]),
                            format!("Created CloudWatch dashboard `{}`", dashboard_name)
                        )
                    }

                    CloudWatchConnectorOp::DeleteDashboard => {
                        client.delete_dashboards().dashboard_names(dashboard_name).send().await?;
                        op_exec_output!(format!("Deleted CloudWatch dashboard `{}`", dashboard_name))
                    }

                    CloudWatchConnectorOp::UpdateDashboardBody { body } => {
                        let dashboard_body = serde_json::to_string(&body).context("Failed to serialize dashboard body")?;

                        client
                            .put_dashboard()
                            .dashboard_name(dashboard_name)
                            .dashboard_body(dashboard_body)
                            .send()
                            .await?;

                        op_exec_output!(format!("Updated CloudWatch dashboard `{}`", dashboard_name))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudWatchResourceAddress::LogGroup(region, log_group_name) => {
                let logs_client = aws_sdk_cloudwatchlogs::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                match op {
                    CloudWatchConnectorOp::CreateLogGroup(log_group) => {
                        let mut request = logs_client.create_log_group().log_group_name(log_group_name);

                        if let Some(kms_key_id) = &log_group.kms_key_id {
                            request = request.kms_key_id(kms_key_id);
                        }

                        request.send().await?;

                        // Set retention if specified
                        if let Some(retention_policy) = &log_group.retention_policy {
                            logs_client
                                .put_retention_policy()
                                .log_group_name(log_group_name)
                                .retention_in_days(retention_policy.retention_in_days)
                                .send()
                                .await?;
                        }

                        let arn = format!("arn:aws:logs:{region}:{account_id}:log-group:{log_group_name}");
                        op_exec_output!(
                            Some([("log_group_arn", Some(arn))]),
                            format!("Created CloudWatch log group `{}`", log_group_name)
                        )
                    }

                    CloudWatchConnectorOp::DeleteLogGroup => {
                        logs_client.delete_log_group().log_group_name(log_group_name).send().await?;
                        op_exec_output!(format!("Deleted CloudWatch log group `{}`", log_group_name))
                    }

                    CloudWatchConnectorOp::UpdateLogGroupRetention { retention_in_days } => {
                        logs_client
                            .put_retention_policy()
                            .log_group_name(log_group_name)
                            .retention_in_days(retention_in_days)
                            .send()
                            .await?;

                        op_exec_output!(format!("Updated retention policy for log group `{}`", log_group_name))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudWatchResourceAddress::LogStream(region, log_group_name, log_stream_name) => {
                let logs_client = aws_sdk_cloudwatchlogs::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                match op {
                    CloudWatchConnectorOp::CreateLogStream(_log_stream) => {
                        logs_client
                            .create_log_stream()
                            .log_group_name(log_group_name)
                            .log_stream_name(log_stream_name)
                            .send()
                            .await?;

                        let arn = format!(
                            "arn:aws:logs:{region}:{account_id}:log-group:{log_group_name}:log-stream:{log_stream_name}"
                        );
                        op_exec_output!(
                            Some([("log_stream_arn", Some(arn))]),
                            format!("Created CloudWatch log stream `{}`", log_stream_name)
                        )
                    }

                    CloudWatchConnectorOp::DeleteLogStream => {
                        logs_client
                            .delete_log_stream()
                            .log_group_name(log_group_name)
                            .log_stream_name(log_stream_name)
                            .send()
                            .await?;

                        op_exec_output!(format!("Deleted CloudWatch log stream `{}`", log_stream_name))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudWatchResourceAddress::EventRule(region, rule_name) => {
                let events_client = aws_sdk_eventbridge::Client::new(
                    &aws_config::defaults(aws_config::BehaviorVersion::latest())
                        .region(aws_config::Region::new(region.clone()))
                        .load()
                        .await,
                );

                match op {
                    CloudWatchConnectorOp::CreateEventRule(rule) => {
                        let mut request = events_client.put_rule().name(rule_name);

                        if let Some(description) = &rule.description {
                            request = request.description(description);
                        }

                        if let Some(schedule_expression) = &rule.schedule_expression {
                            request = request.schedule_expression(schedule_expression);
                        }

                        if let Some(event_pattern) = &rule.event_pattern {
                            let pattern_json =
                                serde_json::to_string(&event_pattern.pattern).context("Failed to serialize event pattern")?;
                            request = request.event_pattern(pattern_json);
                        }

                        request = request.state(aws_sdk_eventbridge::types::RuleState::from(rule.state.as_str()));

                        if let Some(role_arn) = &rule.role_arn {
                            request = request.role_arn(role_arn);
                        }

                        let response = request.send().await?;

                        let arn = response
                            .rule_arn
                            .unwrap_or_else(|| format!("arn:aws:events:{region}:{account_id}:rule/{rule_name}"));

                        op_exec_output!(
                            Some([("rule_arn", Some(arn))]),
                            format!("Created EventBridge rule `{}`", rule_name)
                        )
                    }

                    CloudWatchConnectorOp::DeleteEventRule => {
                        events_client.delete_rule().name(rule_name).send().await?;
                        op_exec_output!(format!("Deleted EventBridge rule `{}`", rule_name))
                    }

                    CloudWatchConnectorOp::UpdateEventRule {
                        description,
                        schedule_expression,
                        event_pattern,
                        state,
                        role_arn,
                    } => {
                        let mut request = events_client.put_rule().name(rule_name);

                        if let Some(description) = description {
                            request = request.description(description);
                        }

                        if let Some(schedule_expression) = schedule_expression {
                            request = request.schedule_expression(schedule_expression);
                        }

                        if let Some(event_pattern) = event_pattern {
                            let pattern_json =
                                serde_json::to_string(&event_pattern.pattern).context("Failed to serialize event pattern")?;
                            request = request.event_pattern(pattern_json);
                        }

                        if let Some(state) = state {
                            request = request.state(aws_sdk_eventbridge::types::RuleState::from(state.as_str()));
                        }

                        if let Some(role_arn) = role_arn {
                            request = request.role_arn(role_arn);
                        }

                        request.send().await?;
                        op_exec_output!(format!("Updated EventBridge rule `{}`", rule_name))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }

            CloudWatchResourceAddress::Metric(region, namespace, metric_name) => {
                let client = self.get_or_init_client(region).await?;

                match op {
                    CloudWatchConnectorOp::PutMetricData {
                        namespace: op_namespace,
                        metric_data,
                    } => {
                        if op_namespace != *namespace {
                            bail!("Namespace mismatch: expected {}, got {}", namespace, op_namespace);
                        }

                        let mut metric_data_items = Vec::new();
                        for data in metric_data {
                            let mut metric_datum =
                                aws_sdk_cloudwatch::types::MetricDatum::builder().metric_name(&data.metric_name);

                            if let Some(dimensions) = &data.dimensions {
                                for dim in dimensions {
                                    metric_datum = metric_datum.dimensions(
                                        aws_sdk_cloudwatch::types::Dimension::builder()
                                            .name(&dim.name)
                                            .value(&dim.value)
                                            .build(),
                                    );
                                }
                            }

                            if let Some(value) = data.value {
                                metric_datum = metric_datum.value(value);
                            }

                            if let Some(timestamp) = data.timestamp {
                                metric_datum = metric_datum.timestamp(aws_smithy_types::DateTime::from_secs(timestamp));
                            }

                            if let Some(unit) = &data.unit {
                                metric_datum = metric_datum.unit(aws_sdk_cloudwatch::types::StandardUnit::from(unit.as_str()));
                            }

                            metric_data_items.push(metric_datum.build())
                        }

                        client
                            .put_metric_data()
                            .namespace(namespace)
                            .set_metric_data(Some(metric_data_items))
                            .send()
                            .await?;

                        op_exec_output!(format!("Put metric data for namespace `{}`", namespace))
                    }

                    _ => Err(invalid_op(&addr, &op)),
                }
            }
        }
    }
}
