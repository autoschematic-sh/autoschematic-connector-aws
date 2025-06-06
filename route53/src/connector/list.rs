use std::path::{Path, PathBuf};

use anyhow::bail;
use autoschematic_core::connector::ResourceAddress;

use crate::{
    addr::Route53ResourceAddress,
    util::{list_hosted_zones, list_resource_record_sets},
};

use super::Route53Connector;

impl Route53Connector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        let mut results = Vec::<PathBuf>::new();
        let Some(ref client) = *self.client.lock().await else {
            bail!("No client")
        };

        let hosted_zones = list_hosted_zones(client).await?;
        for (id, name) in hosted_zones {
            results.push(Route53ResourceAddress::HostedZone(name.clone()).to_path_buf());

            let record_sets = list_resource_record_sets(client, &id).await?;
            for (record_name, r#type) in record_sets {
                results.push(
                    Route53ResourceAddress::ResourceRecordSet(name.clone(), record_name.clone(), r#type.clone()).to_path_buf(),
                );
            }
        }

        Ok(results)
    }
}
