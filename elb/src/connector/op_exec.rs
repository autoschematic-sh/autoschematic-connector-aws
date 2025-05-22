use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error::{AutoschematicError, AutoschematicErrorType},
    op_exec_output,
};
use aws_sdk_route53::types::{AliasTarget, Change, ChangeBatch, RrType};

use crate::{addr::TemplateResourceAddress, op::TemplateConnectorOp};

use super::TemplateConnector;

impl TemplateConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!();
    }
}
