use std::path::Path;

use autoschematic_core::{connector::{OpPlanOutput, ResourceAddress}, connector_op, util::RON};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::ApiGatewayV2ResourceAddress, op::ApiGatewayV2ConnectorOp, resource::{HostedZone, RecordSet}};

use super::ApiGatewayV2Connector;


impl ApiGatewayV2Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!();
    }
}