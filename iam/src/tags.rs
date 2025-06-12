use std::collections::HashMap;

use autoschematic_core::connector::{Connector, ConnectorOp};
use serde::{Deserialize, Serialize};

use aws_sdk_iam::types::Tag;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct Tags(HashMap<String, String>);

impl From<Option<Vec<Tag>>> for Tags {
    fn from(value: Option<Vec<Tag>>) -> Self {
        match value {
            Some(mut tags) => {
                tags.sort_by_key(|t| t.key.clone());
                let mut out_map = HashMap::new();
                for tag in tags {
                    out_map.insert(tag.key, tag.value);
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
            out_vec.push(Tag::builder().key(k).value(v).build().unwrap());
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// From a pair of hashmap, determine the set of aws_iam::Tag structs to pass to untag and set_tags respectively
pub fn tag_diff(old_tags: &Tags, new_tags: &Tags) -> anyhow::Result<(Vec<String>, Vec<Tag>)> {
    let mut untag_keys = Vec::new();
    for (k, _) in &old_tags.0 {
        if !new_tags.0.contains_key(k) {
            untag_keys.push(k.to_string());
        }
    }

    let mut new_tagset = Vec::new();
    for (key, new_value) in &new_tags.0 {
        if !old_tags.0.contains_key(key) {
            if let Ok(tag) = Tag::builder().key(key).value(new_value).build() {
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
