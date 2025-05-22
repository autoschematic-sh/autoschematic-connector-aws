use std::path::Path;

use autoschematic_core::{connector::{OpPlanOutput, ResourceAddress}, connector_op, util::RON};

use autoschematic_core::connector::ConnectorOp;

use crate::{addr::TemplateResourceAddress, op::TemplateConnectorOp, resource::{HostedZone, RecordSet}};

use super::TemplateConnector;


impl TemplateConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!();
    }
}