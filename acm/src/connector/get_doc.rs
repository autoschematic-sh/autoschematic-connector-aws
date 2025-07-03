use autoschematic_core::connector::{DocIdent, GetDocOutput};
use anyhow::Result;

use super::AcmConnector;

impl AcmConnector {
    pub async fn do_get_doc(&self, ident: DocIdent) -> Result<Option<GetDocOutput>> {
        Ok(None)
    }
}
