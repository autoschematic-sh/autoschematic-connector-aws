
use aws_sdk_elasticloadbalancingv2::types::Tag;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

// Define Tags similar to S3 implementation
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Tags(IndexMap<String, String>);

impl From<Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>>> for Tags {
    fn from(value: Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>>) -> Self {
        if let Some(mut tags) = value {
            tags.sort_by_key(|t| t.key.clone());
            let mut out_map = IndexMap::new();
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

impl Into<Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>>> for Tags {
    fn into(self) -> std::option::Option<Vec<aws_sdk_elasticloadbalancingv2::types::Tag>> {
        let mut out_vec = Vec::new();

        for (k, v) in self.0 {
            out_vec.push(
                aws_sdk_elasticloadbalancingv2::types::Tag::builder()
                    .key(k)
                    .value(v)
                    .build(),
            );
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
fn s3_tag_diff(
    old_tags: &Tags,
    new_tags: &Tags,
) -> anyhow::Result<(Vec<String>, Vec<aws_sdk_elasticloadbalancingv2::types::Tag>)> {
    let mut untag_keys = Vec::new();
    for (k, _) in &old_tags.0 {
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
