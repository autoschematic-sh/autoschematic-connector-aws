use std::collections::HashMap;

use aws_sdk_s3::types::Tag;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Tags(HashMap<String, String>);

impl From<Vec<aws_sdk_s3::types::Tag>> for Tags {
    fn from(mut tags: Vec<aws_sdk_s3::types::Tag>) -> Self {
        tags.sort_by_key(|t| t.key.clone());
        let mut out_map = HashMap::new();
        for tag in tags {
            out_map.insert(tag.key, tag.value);
        }
        Tags(out_map)
    }
}

impl From<Tags> for Option<Vec<aws_sdk_s3::types::Tag>> {
    fn from(val: Tags) -> Self {
        let mut out_vec = Vec::new();

        for (k, v) in val.0 {
            out_vec.push(aws_sdk_s3::types::Tag::builder().key(k).value(v).build().unwrap());
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// From a pair of hashmap, determine the set of aws_s3::Tag structs to pass to untag and set_tags respectively
fn s3_tag_diff(old_tags: &Tags, new_tags: &Tags) -> anyhow::Result<(Vec<String>, Vec<aws_sdk_s3::types::Tag>)> {
    let mut untag_keys = Vec::new();
    for k in old_tags.0.keys() {
        if !new_tags.0.contains_key(k) {
            untag_keys.push(k.to_string());
        }
    }

    let mut new_tagset = Vec::new();
    for (key, new_value) in &new_tags.0 {
        if !old_tags.0.contains_key(key) {
            if let Ok(tag) = aws_sdk_s3::types::Tag::builder().key(key).value(new_value).build() {
                new_tagset.push(tag);
            }
        } else if let Some(old_value) = old_tags.0.get(key) {
            if old_value != new_value {
                if let Ok(tag) = Tag::builder().key(key).value(new_value).build() {
                    new_tagset.push(tag);
                }
            }
        }
    }

    Ok((untag_keys, new_tagset))
}
