use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::TemplateResourceAddress,
    resource::{HostedZone, RecordSet, TemplateResource},
};

use super::TemplateConnector;

impl TemplateConnector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
