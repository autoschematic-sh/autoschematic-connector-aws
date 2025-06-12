use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::{
    addr::EfsResourceAddress,
    util::{list_hosted_zones, list_resource_record_sets},
};

use super::EfsConnector;

impl EfsConnector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        todo!();
    }
}
