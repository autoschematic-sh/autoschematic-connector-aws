use std::collections::HashMap;

use aws_sdk_cloudwatch::types::Tag;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Tags(HashMap<String, String>);

impl From<Option<Vec<Tag>>> for Tags {
    fn from(value: Option<Vec<Tag>>) -> Self {
        match value {
            Some(mut tags) => {
                tags.sort_by_key(|t| t.key.clone());
                let mut out_map = HashMap::new();
                for tag in tags {
                    if let (Some(key), Some(value)) = (tag.key, tag.value) {
                        out_map.insert(key, value);
                    }
                }
                Tags(out_map)
            }
            None => Tags(HashMap::new()),
        }
    }
}

impl From<Tags> for Option<Vec<Tag>> {
    fn from(val: Tags) -> Self {
        let mut out_vec = Vec::new();

        for (k, v) in val.0 {
            out_vec.push(Tag::builder().key(k).value(v).build());
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// From a pair of hashmap, determine the set of aws_sdk_cloudwatch::Tag structs to pass to untag and set_tags respectively
pub fn tag_diff(old_tags: &Tags, new_tags: &Tags) -> anyhow::Result<(Vec<String>, Vec<Tag>)> {
    let mut untag_keys = Vec::new();
    for k in old_tags.0.keys() {
        if !new_tags.0.contains_key(k) {
            untag_keys.push(k.to_string());
        }
    }

    let mut new_tagset = Vec::new();
    for (key, new_value) in &new_tags.0 {
        if !old_tags.0.contains_key(key) {
            new_tagset.push(Tag::builder().key(key).value(new_value).build());
        } else if let Some(old_value) = old_tags.0.get(key) {
            if old_value != new_value {
                new_tagset.push(Tag::builder().key(key).value(new_value).build());
            }
        }
    }

    Ok((untag_keys, new_tagset))
}
