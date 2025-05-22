use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::S3ResourceAddress,
    resource::{HostedZone, RecordSet, S3Resource},
};

use super::S3Connector;

impl S3Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
