use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::connector::OpExecOutput;

use crate::{addr::ApiGatewayV2ResourceAddress, op::ApiGatewayV2ConnectorOp};

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!();
    }
}
