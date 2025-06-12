use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};
use aws_sdk_route53::types::RrType;

use crate::{
    addr::ApiGatewayV2ResourceAddress,
    resource::{HostedZone, RecordSet, ApiGatewayV2Resource},
};

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
