// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    /// use mz_ore::str::StrExt;
    ///
    /// let name = "bob";
    /// let message = format!("unknown user {}", name.quoted());
    /// assert_eq!(message, r#"unknown user "bob""#);
    /// ```
    ///
    /// In a pathological case:
    ///
    /// ```
    /// use mz_ore::str::StrExt;
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

/// Creates a type whose [`fmt::Display`] implementation outputs item preceded
/// by `open` and followed by `close`.
pub fn bracketed<'a, D>(open: &'a str, close: &'a str, contents: D) -> impl fmt::Display + 'a
where
    D: fmt::Display + 'a,
{
    struct Bracketed<'a, D> {
        open: &'a str,
        close: &'a str,
        contents: D,
    }

    impl<'a, D> fmt::Display for Bracketed<'a, D>
    where
        D: fmt::Display,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}{}{}", self.open, self.contents, self.close)
        }
    }

    Bracketed {
        open,
        close,
        contents,
    }
}

/// Creates a type whose [`fmt::Display`] implementation outputs each item in
/// `iter` separated by `separator`.
pub fn separated<'a, I>(separator: &'a str, iter: I) -> impl fmt::Display + 'a
where
    I: IntoIterator,
    I::IntoIter: Clone + 'a,
    I::Item: fmt::Display + 'a,
{
    struct Separated<'a, I> {
        separator: &'a str,
        iter: I,
    }

    impl<'a, I> fmt::Display for Separated<'a, I>
    where
        I: Iterator + Clone,
        I::Item: fmt::Display,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            for (i, item) in self.iter.clone().enumerate() {
                if i != 0 {
                    write!(f, "{}", self.separator)?;
                }
                write!(f, "{}", item)?;
            }
            Ok(())
        }
    }

    Separated {
        separator,
        iter: iter.into_iter(),
    }
}

/// A helper struct to keep track of indentation levels.
///
/// This will be most often used as part of the rendering context
/// type for various `Display$Format` implementation.
#[derive(Debug, Clone)]
pub struct Indent {
    unit: String,
    buff: String,
}

impl Indent {
    /// Construct a new `Indent` where one level is represented
    /// by the given `unit` repeated `step` times.
    pub fn new(unit: char, step: usize) -> Indent {
        Indent {
            unit: std::iter::repeat(unit).take(step).collect::<String>(),
            buff: String::with_capacity(unit.len_utf8()),
        }
    }

    fn inc(&mut self, rhs: usize) {
        for _ in 0..rhs {
            self.buff += &self.unit;
        }
    }

    fn dec(&mut self, rhs: usize) {
        let tail = rhs.saturating_mul(self.unit.len());
        let head = self.buff.len().saturating_sub(tail);
        self.buff.truncate(head);
    }
}

/// Convenience methods for pretty-printing based on indentation
/// that are automatically available for context objects that can
/// be mutably referenced as an [`Indent`] instance.
pub trait IndentLike {
    /// Print a block of code defined in `f` one step deeper
    /// from the current [`Indent`].
    fn indented<F>(&mut self, f: F) -> fmt::Result
    where
        F: FnMut(&mut Self) -> fmt::Result;
}

impl IndentLike for Indent {
    fn indented<F>(&mut self, mut f: F) -> fmt::Result
    where
        F: FnMut(&mut Self) -> fmt::Result,
    {
        *self += 1;
        let result = f(self);
        *self -= 1;
        result
    }
}

impl<T: AsMut<Indent>> IndentLike for T {
    fn indented<F>(&mut self, mut f: F) -> fmt::Result
    where
        F: FnMut(&mut Self) -> fmt::Result,
    {
        *self.as_mut() += 1;
        let result = f(self);
        *self.as_mut() -= 1;
        result
    }
}

impl Default for Indent {
    fn default() -> Self {
        Indent::new(' ', 2)
    }
}

impl std::ops::AddAssign<usize> for Indent {
    fn add_assign(&mut self, rhs: usize) {
        self.inc(rhs)
    }
}

impl std::ops::SubAssign<usize> for Indent {
    fn sub_assign(&mut self, rhs: usize) {
        self.dec(rhs)
    }
}

impl fmt::Display for Indent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.buff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indent() {
        let mut indent = Indent::new('~', 3);
        indent += 1;
        assert_eq!(indent.to_string(), "~~~".to_string());
        indent += 3;
        assert_eq!(indent.to_string(), "~~~~~~~~~~~~".to_string());
        indent -= 2;
        assert_eq!(indent.to_string(), "~~~~~~".to_string());
        indent -= 4;
        assert_eq!(indent.to_string(), "".to_string());
        indent += 1;
        assert_eq!(indent.to_string(), "~~~".to_string());
    }
}
