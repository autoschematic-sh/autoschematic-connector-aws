use std::{collections::HashMap, path::Path};

use anyhow::bail;
use autoschematic_core::{
    connector::{ConnectorOp, OpExecOutput, ResourceAddress},
    error::{AutoschematicError, AutoschematicErrorType},
    op_exec_output,
};
use aws_sdk_route53::types::{AliasTarget, Change, ChangeBatch, RrType};

use crate::{addr::CloudWatchResourceAddress, op::CloudWatchConnectorOp};

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_op_exec(&self, addr: &Path, op: &str) -> Result<OpExecOutput, anyhow::Error> {
        todo!();
    }
}
