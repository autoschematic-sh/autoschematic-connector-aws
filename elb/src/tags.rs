use std::collections::HashMap;

use aws_sdk_elasticloadbalancingv2::types::Tag;
use serde::{Deserialize, Serialize};

// Define Tags similar to S3 implementation
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tags(HashMap<String, String>);

impl From<Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>>> for Tags {
    fn from(value: Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>>) -> Self {
        if let Some(tags) = value {
            let mut out_map = HashMap::new();
            for tag in tags {
                if let (Some(tag_key), Some(tag_value)) = (tag.key, tag.value) {
                    out_map.insert(tag_key, tag_value);
                }
            }
            Tags(out_map)
        } else {
            Tags::default()
        }
    }
}

impl From<Tags> for Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>> {
    fn from(val: Tags) -> Self {
        let mut out_vec = Vec::new();

        for (k, v) in val.0 {
            out_vec.push(aws_sdk_elasticloadbalancingv2::types::Tag::builder().key(k).value(v).build());
        }

        Some(out_vec)
    }
}

impl Tags {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

// From a pair of hashmaps, determine the set of Tag structs to pass to untag and set_tags respectively
pub fn tag_diff(
    old_tags: &Tags,
    new_tags: &Tags,
) -> anyhow::Result<(Vec<String>, Vec<aws_sdk_elasticloadbalancingv2::types::Tag>)> {
    let mut untag_keys = Vec::new();
    for k in old_tags.0.keys() {
        if !new_tags.0.contains_key(k) {
            untag_keys.push(k.to_string());
        }
    }

    let mut new_tagset = Vec::new();
    for (key, new_value) in &new_tags.0 {
        if !old_tags.0.contains_key(key) {
            let tag = aws_sdk_elasticloadbalancingv2::types::Tag::builder()
                .key(key)
                .value(new_value)
                .build();
            new_tagset.push(tag);
        } else if let Some(old_value) = old_tags.0.get(key) {
            if old_value != new_value {
                let tag = Tag::builder().key(key).value(new_value).build();
                new_tagset.push(tag);
            }
        }
    }

    Ok((untag_keys, new_tagset))
}
