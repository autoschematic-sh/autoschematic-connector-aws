use std::{ffi::OsString, os::unix::ffi::{OsStringExt}};


pub fn optional_string_from_utf8(s: Option<OsString>) -> anyhow::Result<Option<String>> {
    match s {
        Some(s) => {
            Ok(Some(String::from_utf8(s.into_vec())?))
        }
        None => Ok(None)
    }
}