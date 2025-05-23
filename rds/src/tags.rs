use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Tags(HashMap<String, String>);

impl From<&Option<Vec<aws_sdk_rds::types::Tag>>> for Tags {
    fn from(tags: &Option<Vec<aws_sdk_rds::types::Tag>>) -> Self {
        if let Some(tags) = tags {
            let mut out_map = HashMap::new();
            for tag in tags {
                let Some(ref key) = tag.key else {
                    continue;
                };
                let Some(ref value) = tag.value else {
                    continue;
                };
                out_map.insert(key.clone(), value.clone());
            }
            Tags(out_map)
        } else {
            Tags::default()
        }
    }
}

impl From<Option<Vec<aws_sdk_rds::types::Tag>>> for Tags {
    fn from(tags: Option<Vec<aws_sdk_rds::types::Tag>>) -> Self {
        if let Some(tags) = tags {
            let mut out_map = HashMap::new();
            for tag in tags {
                let Some(key) = tag.key else {
                    continue;
                };
                let Some(value) = tag.value else {
                    continue;
                };
                out_map.insert(key, value);
            }
            Tags(out_map)
        } else {
            Tags::default()
        }
    }
}

impl From<Tags> for Option<Vec<aws_sdk_rds::types::Tag>> {
    fn from(val: Tags) -> Self {
        let mut out_vec = Vec::new();

        for (k, v) in val.0 {
            out_vec.push(aws_sdk_rds::types::Tag::builder().key(k).value(v).build())
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}
