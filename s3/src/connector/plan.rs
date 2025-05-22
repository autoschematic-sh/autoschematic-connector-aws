use std::path::Path;

use autoschematic_core::{connector::{OpPlanOutput, ResourceAddress}, connector_op, util::RON};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::S3ResourceAddress, op::S3ConnectorOp, resource::{HostedZone, RecordSet}};

use super::S3Connector;


impl S3Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!();
    }
}