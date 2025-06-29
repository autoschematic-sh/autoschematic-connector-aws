use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::Route53ResourceAddress,
    resource::{AliasTarget, HostedZone, RecordSet, Route53Resource},
};

use super::Route53Connector;

impl Route53Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        let addr = Route53ResourceAddress::from_path(addr)?;
        let Some(ref client) = *self.client.lock().await else {
            bail!("No client")
        };

        match addr {
            Route53ResourceAddress::HostedZone(name) => {
                let hz = client.list_hosted_zones_by_name().dns_name(name).send().await?;

                let Some(hz) = hz.hosted_zones.first() else {
                    return Ok(None);
                };

                let hz_config = HostedZone {};

                let mut outputs = HashMap::new();
                outputs.insert(String::from("id"), hz.id.clone());

                Ok(Some(GetResourceOutput {
                    resource_definition: Route53Resource::HostedZone(hz_config).to_bytes()?,
                    outputs: Some(outputs),
                }))
            }
            Route53ResourceAddress::ResourceRecordSet(hosted_zone, name, r#type) => {
                let hz = client
                    .list_hosted_zones_by_name()
                    // .dns_name(name.strip_suffix('.').unwrap())
                    .dns_name(hosted_zone.clone())
                    .send()
                    .await?;

                match hz.hosted_zones.first() {
                    Some(hz) if hz.name == hosted_zone => {
                        let rr_type = RrType::try_parse(&r#type)?;

                        let rec = client
                            .list_resource_record_sets()
                            .set_hosted_zone_id(Some(hz.id.clone()))
                            .set_start_record_name(Some(name.to_string()))
                            .start_record_type(rr_type.clone())
                            .send()
                            .await?;

                        match rec.resource_record_sets.first() {
                            Some(rec) if rec.name == *name && rec.r#type == rr_type => {
                                // let i = rec.region
                                let record_set = RecordSet {
                                    ttl: rec.ttl,
                                    alias_target: rec
                                        .alias_target
                                        .as_ref()
                                        .map(|alias_target| AliasTarget {
                                            dns_name: alias_target.dns_name.clone(),
                                            hosted_zone_id: alias_target.hosted_zone_id.clone(),
                                            evaluate_target_health: alias_target.evaluate_target_health
                                        }
                                        ),
                                    resource_records: rec
                                        .resource_records
                                        .as_ref()
                                        .map(|records| records.iter().map(|r| r.value.clone()).collect()),
                                };

                                Ok(Some(GetResourceOutput {
                                    resource_definition: Route53Resource::RecordSet(record_set).to_bytes()?,
                                    outputs: None,
                                }))
                            }
                            _ => Ok(None),
                        }
                    }
                    _ => Ok(None),
                }
            }

            _ => Ok(None),
        }
    }
}
