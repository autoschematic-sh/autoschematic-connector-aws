use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::CloudWatchResourceAddress,
    resource::{HostedZone, RecordSet, CloudWatchResource},
};

use super::CloudWatchConnector;

impl CloudWatchConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
