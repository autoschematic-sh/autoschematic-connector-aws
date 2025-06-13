use std::path::Path;

use autoschematic_core::{
    connector::{OpPlanOutput, ResourceAddress},
    connector_op,
    util::RON,
};

use autoschematic_core::connector::ConnectorOp;

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_plan(
        &self,
        addr: &Path,
        current: Option<Vec<u8>>,
        desired: Option<Vec<u8>>,
    ) -> Result<Vec<OpPlanOutput>, anyhow::Error> {
        todo!();
    }
}
