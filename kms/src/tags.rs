use std::collections::HashMap;

use aws_sdk_kms::types::Tag;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Tags(HashMap<String, String>);

impl From<Option<Vec<aws_sdk_kms::types::Tag>>> for Tags {
    fn from(value: Option<Vec<aws_sdk_kms::types::Tag>>) -> Self {
        if let Some(mut tags) = value {
            tags.sort_by_key(|t| t.tag_key.clone());
            let mut out_map = HashMap::new();
            for tag in tags {
                out_map.insert(tag.tag_key, tag.tag_value);
            }
            Tags(out_map)
        } else {
            Tags(HashMap::default())
        }
    }
}

impl From<Tags> for Option<Vec<aws_sdk_kms::types::Tag>> {
    fn from(val: Tags) -> Self {
        let mut out_vec = Vec::new();

        for (k, v) in val.0 {
            out_vec.push(aws_sdk_kms::types::Tag::builder().tag_key(k).tag_value(v).build().unwrap());
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// From a pair of hashmaps, determine the set of aws_kms::Tag structs to add and remove
pub fn kms_tag_diff(old_tags: &Tags, new_tags: &Tags) -> anyhow::Result<(Vec<String>, Vec<aws_sdk_kms::types::Tag>)> {
    let mut remove_keys = Vec::new();
    for k in old_tags.0.keys() {
        if !new_tags.0.contains_key(k) {
            remove_keys.push(k.to_string());
        }
    }

    let mut add_tags = Vec::new();
    for (key, new_value) in &new_tags.0 {
        if !old_tags.0.contains_key(key) {
            if let Ok(tag) = aws_sdk_kms::types::Tag::builder().tag_key(key).tag_value(new_value).build() {
                add_tags.push(tag);
            }
        } else if let Some(old_value) = old_tags.0.get(key) {
            if old_value != new_value {
                if let Ok(tag) = Tag::builder().tag_key(key).tag_value(new_value).build() {
                    add_tags.push(tag);
                }
            }
        }
    }

    Ok((remove_keys, add_tags))
}
