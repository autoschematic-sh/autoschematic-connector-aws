use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::CloudFrontResourceAddress,
    resource::{HostedZone, RecordSet, CloudFrontResource},
};

use super::CloudFrontConnector;

impl CloudFrontConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
