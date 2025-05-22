use std::path::Path;

use autoschematic_core::{connector::{OpPlanOutput, ResourceAddress}, connector_op, util::RON};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::Route53ResourceAddress, op::Route53ConnectorOp, resource::{HostedZone, RecordSet}};

use super::Route53Connector;


impl Route53Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {

        let addr = Route53ResourceAddress::from_path(addr)?;

        match addr {
            Route53ResourceAddress::HostedZone(name) => match (current, desired) {
                (None, None) => Ok(vec![]),
                (None, Some(new_zone)) => {
                    let new_zone: HostedZone = RON.from_str(&new_zone).unwrap();
                    Ok(vec![connector_op!(
                        Route53ConnectorOp::CreateHostedZone(new_zone),
                        format!("Create new hosted zone {}", name)
                    )])
                }
                (Some(old_zone), None) => {
                    let _old_zone: HostedZone = RON.from_str(&old_zone).unwrap();
                    Ok(vec![connector_op!(
                        Route53ConnectorOp::DeleteHostedZone,
                        format!("DELETE hosted zone {}", name)
                    )])
                }
                (Some(old_zone), Some(new_zone)) => {
                    let old_zone: HostedZone = RON.from_str(&old_zone).unwrap();
                    let new_zone: HostedZone = RON.from_str(&new_zone).unwrap();
                    //  TODO can we put a nice diff here?
                    Ok(vec![connector_op!(
                        Route53ConnectorOp::ModifyHostedZone(old_zone, new_zone),
                        format!("MODIFY hosted zone {}", name)
                    )])
                }
            },
            Route53ResourceAddress::ResourceRecordSet(hosted_zone, name, r#type) => {
                match (current, desired) {
                    (None, None) => Ok(vec![]),
                    (None, Some(new_record)) => {
                        let new_record: RecordSet = RON.from_str(&new_record)?;
                        Ok(vec![connector_op!(
                            Route53ConnectorOp::CreateResourceRecordSet(new_record),
                            format!(
                                "Create {} Record at {} in hosted zone {}",
                                r#type, name, hosted_zone
                            )
                        )])
                    }
                    (Some(old_record), None) => {
                        let old_record: RecordSet = RON.from_str(&old_record)?;
                        Ok(vec![connector_op!(
                            Route53ConnectorOp::DeleteResourceRecordSet(old_record),
                            format!(
                                "DELETE {} Record at {} in hosted zone {}",
                                r#type, name, hosted_zone
                            )
                        )])
                    }
                    (Some(old_record), Some(new_record)) if old_record != new_record => {
                        let old_record: RecordSet = RON.from_str(&old_record)?;
                        let new_record: RecordSet = RON.from_str(&new_record)?;
                        Ok(vec![
                            connector_op!(
                                Route53ConnectorOp::DeleteResourceRecordSet(old_record,),
                                format!(
                                    "DELETE {} Record at {} in hosted zone {}",
                                    r#type, name, hosted_zone
                                )
                            ),
                            connector_op!(
                                Route53ConnectorOp::CreateResourceRecordSet(new_record,),
                                format!(
                                    "Create {} Record at {} in hosted zone {}",
                                    r#type, name, hosted_zone
                                )
                            ),
                        ])
                    }
                    _ => Ok(vec![]),
                }
            }
            // Some(Route53ResourceAddress::HealthCheck(name)) => {
            //     match (current, desired) {
            //         (None, None) => Ok(vec![]),
            //         (None, Some(health_check)) => {
            //             Ok(vec![Route53ConnectorOp::CreateHostedZone(new_zone)])
            //         }
            //         (Some(Route53Resource::HostedZone(old_zone)), None) => {
            //             Ok(vec![Route53ConnectorOp::DeleteHostedZone(old_zone)])
            //         }
            //         (
            //             Some(Route53Resource::HostedZone(old_zone)),
            //             Some(Route53Resource::HostedZone(new_zone)),
            //         ) => Ok(vec![Route53ConnectorOp::ModifyHostedZone(
            //             old_zone, new_zone,
            //         )]),
            //         _ => {
            //             // Should never happen?
            //             Ok(vec![])
            //         }
            //     }
            // }
            _ => Ok(vec![]),
        }
    }
}