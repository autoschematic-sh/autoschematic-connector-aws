
use autoschematic_core::connector::{DocIdent, GetDocResponse};
use documented::{Documented, DocumentedFields};

use crate::resource;

use super::IamConnector;

impl IamConnector {
    pub async fn do_get_doc(&self, ident: DocIdent) -> Result<Option<GetDocResponse>, anyhow::Error> {
        eprintln!("get_doc: {ident:?}");

        match ident {
            DocIdent::Struct { name } => {
                match name.as_str() {
                    "IamRole" => Ok(Some(resource::IamRole::DOCS.into())),
                    _ => Ok(None),
                }
            }
            DocIdent::Field { parent, name } => {
                match parent.as_str() {
                    "IamRole" => Ok(Some(resource::IamRole::get_field_docs(name)?.into())),
                    _ => Ok(None),
                }
            }
        }
    }
}
