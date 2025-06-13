use std::{collections::HashMap, path::Path};

use autoschematic_core::connector::{GetResourceOutput, Resource, ResourceAddress};

use crate::addr::ApiGatewayV2ResourceAddress;

use super::ApiGatewayV2Connector;

impl ApiGatewayV2Connector {
    pub async fn do_get(&self, addr: &Path) -> Result<Option<GetResourceOutput>, anyhow::Error> {
        todo!();
    }
}
