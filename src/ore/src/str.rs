// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! String utilities.

use std::fmt::{self, Write};
use std::ops::Deref;

/// Extension methods for [`str`].
pub trait StrExt {
    /// Wraps the string slice in a type whose display implementation renders
    /// the string surrounded by double quotes with any inner double quote
    /// characters escaped.
    ///
    /// # Examples
    ///
    /// In the standard case, when the wrapped string does not contain any
    /// double quote characters:
    ///
    /// ```
    /// use ore::str::StrExt;
    ///
    /// let name = "bob";
    /// let message = format!("unknown user {}", name.quoted());
    /// assert_eq!(message, r#"unknown user "bob""#);
    /// ```
    ///
    /// In a pathological case:
    ///
    /// ```
    /// use ore::str::StrExt;
    ///
    /// let name = r#"b@d"inp!t""#;
    /// let message = format!("unknown user {}", name.quoted());
    /// assert_eq!(message, r#"unknown user "b@d\"inp!t\"""#);
    /// ```
    fn quoted(&self) -> QuotedStr;
}

impl StrExt for str {
    fn quoted(&self) -> QuotedStr {
        QuotedStr(self)
    }
}

/// Displays a string slice surrounded by double quotes with any inner double
/// quote characters escaped.
///
/// Constructed by [`StrExt::quoted`].
#[derive(Debug)]
pub struct QuotedStr<'a>(&'a str);

impl<'a> fmt::Display for QuotedStr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_char('"')?;
        for c in self.chars() {
            match c {
                '"' => f.write_str("\\\"")?,
                _ => f.write_char(c)?,
            }
        }
        f.write_char('"')
    }
}

impl<'a> Deref for QuotedStr<'a> {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}
