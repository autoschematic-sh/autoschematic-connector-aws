use std::path::Path;

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    op_exec_output,
};
use aws_sdk_route53::types::{AliasTarget, Change, ChangeBatch, RrType};

use crate::{addr::Route53ResourceAddress, op::Route53ConnectorOp};

use super::Route53Connector;

impl Route53Connector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        let addr = Route53ResourceAddress::from_path(addr)?;
        let op = Route53ConnectorOp::from_str(op)?;

        let Some(ref client) = *self.client.lock().await else {
            bail!("No client")
        };

        match addr {
            Route53ResourceAddress::ResourceRecordSet(hosted_zone_name, record_set_name, r#type) => {
                match op {
                    Route53ConnectorOp::CreateResourceRecordSet(record_set) => {
                        let hz = client
                            .list_hosted_zones_by_name()
                            .dns_name(hosted_zone_name.clone())
                            .send()
                            .await?;

                        let mut record_set_builder = aws_sdk_route53::types::ResourceRecordSet::builder()
                            .name(record_set_name)
                            .r#type(RrType::try_parse(&r#type)?);

                        if let Some(ttl) = record_set.ttl {
                            record_set_builder = record_set_builder.ttl(ttl);
                        }

                        if let Some(resource_records) = record_set.resource_records {
                            for rec in resource_records {
                                let resource_record_builder = aws_sdk_route53::types::ResourceRecord::builder().value(rec);
                                record_set_builder = record_set_builder.resource_records(resource_record_builder.build()?);
                            }
                        }

                        if let Some(alias_target) = record_set.alias_target {
                            let alias_target_builder = AliasTarget::builder().dns_name(alias_target);
                            record_set_builder = record_set_builder.alias_target(alias_target_builder.build()?);
                        }

                        match hz.hosted_zones.first() {
                            Some(hz) if hz.name == hosted_zone_name => {
                                client
                                    .change_resource_record_sets()
                                    .hosted_zone_id(hz.id.clone())
                                    .change_batch(
                                        ChangeBatch::builder()
                                            .changes(
                                                Change::builder()
                                                    .action(aws_sdk_route53::types::ChangeAction::Create)
                                                    .resource_record_set(record_set_builder.build()?)
                                                    .build()?,
                                            )
                                            .build()?,
                                    )
                                    .send()
                                    .await?;
                            }
                            _ => {
                                bail!("Hosted zone {} not found!", hosted_zone_name)
                            }
                        }
                        op_exec_output!(format!("Created {} Record on Hosted Zone {}", r#type, hosted_zone_name))
                        // Ok(OpExecOutput {
                        //     outputs: Some(HashMap::new()),
                        //     friendly_message: Some(format!(
                        //         "Created {} Record on Hosted Zone {}",
                        //         r#type, hosted_zone_name
                        //     )),
                        // })
                    }
                    Route53ConnectorOp::DeleteResourceRecordSet(record_set) => {
                        let hz = client
                            .list_hosted_zones_by_name()
                            .dns_name(hosted_zone_name.clone())
                            .send()
                            .await?;

                        let mut record_set_builder = aws_sdk_route53::types::ResourceRecordSet::builder()
                            .name(record_set_name)
                            .r#type(RrType::try_parse(&r#type)?);

                        if let Some(ttl) = record_set.ttl {
                            record_set_builder = record_set_builder.ttl(ttl);
                        }

                        if let Some(resource_records) = record_set.resource_records {
                            for rec in resource_records {
                                let resource_record_builder = aws_sdk_route53::types::ResourceRecord::builder().value(rec);
                                record_set_builder = record_set_builder.resource_records(resource_record_builder.build()?);
                            }
                        }

                        if let Some(alias_target) = record_set.alias_target {
                            let alias_target_builder = AliasTarget::builder().dns_name(alias_target);
                            record_set_builder = record_set_builder.alias_target(alias_target_builder.build()?);
                        }

                        match hz.hosted_zones.first() {
                            Some(hz) if hz.name == hosted_zone_name => {
                                client
                                    .change_resource_record_sets()
                                    .hosted_zone_id(hz.id.clone())
                                    .change_batch(
                                        ChangeBatch::builder()
                                            .changes(
                                                Change::builder()
                                                    .action(aws_sdk_route53::types::ChangeAction::Delete)
                                                    .resource_record_set(record_set_builder.build()?)
                                                    .build()?,
                                            )
                                            .build()?,
                                    )
                                    .send()
                                    .await?;
                            }
                            _ => {
                                bail!("Hosted zone {} not found!", hosted_zone_name)
                            }
                        }
                        op_exec_output!(format!("Deleted {} Record on Hosted Zone {}", r#type, hosted_zone_name))
                    }
                    _ => todo!(),
                }
            }
            Route53ResourceAddress::HostedZone(_) => todo!(),
            Route53ResourceAddress::HealthCheck(_) => todo!(),
        }
    }
}
