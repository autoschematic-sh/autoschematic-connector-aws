use std::path::{Path, PathBuf};

use autoschematic_core::connector::ResourceAddress;

use crate::{
    addr::S3ResourceAddress,
    util::{list_hosted_zones, list_resource_record_sets},
};

use super::S3Connector;

impl S3Connector {
    pub async fn do_list(&self, _subpath: &Path) -> Result<Vec<PathBuf>, anyhow::Error> {
        todo!();
    }
}
