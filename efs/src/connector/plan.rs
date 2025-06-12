use std::path::Path;

use autoschematic_core::{
    connector::{OpPlanOutput, ResourceAddress},
    connector_op,
    util::RON,
};

use autoschematic_core::connector::ConnectorOp;

use crate::{
    addr::EfsResourceAddress,
    op::EfsConnectorOp,
    resource::{HostedZone, RecordSet},
};

use super::EfsConnector;

impl EfsConnector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<String>,
        desired: Option<String>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!();
    }
}
