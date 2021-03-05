// Copyright 2020 Sergio Benitez. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0
//
// This file is derived from the uncased project, available at
// https://github.com/SergioBenitez/uncased. It was incorporated
// directly into Materialize on November 6, 2020.
//
// The original source code is subject to the terms of the Apache
// License, Version 2.0 or the MIT license. Copies of both licenses can
// be found in the LICENSE file at the root of this repository.

//! ASCII utilities.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::slice::SliceIndex;

use phf_shared::{FmtConst, PhfHash};

/// A cost-free reference to an uncased (case-insensitive, case-preserving)
/// ASCII string.
///
/// This is typically created from an `&str` as follows:
///
/// ```rust
/// use ore::ascii::UncasedStr;
///
/// let ascii_ref: &UncasedStr = "Hello, world!".into();
/// ```
// TODO(benesch): remove this type and depend on uncased instead if phf gets
// support for the `uncased` crate. See: https://github.com/sfackler/rust-phf/pull/197.
#[derive(Debug)]
#[repr(transparent)]
pub struct UncasedStr(str);

impl UncasedStr {
    /// Cost-free conversion from an `&str` reference to an `UncasedStr`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use ore::ascii::UncasedStr;
    ///
    /// let uncased_str = UncasedStr::new("Hello!");
    /// assert_eq!(uncased_str, "hello!");
    /// assert_eq!(uncased_str, "Hello!");
    /// assert_eq!(uncased_str, "HeLLo!");
    /// ```
    pub fn new(string: &str) -> &UncasedStr {
        // This is a `newtype`-like transformation. `repr(transparent)` ensures
        // that this is safe and correct.
        unsafe { &*(string as *const str as *const UncasedStr) }
    }

    /// Returns `self` as an `&str`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use ore::ascii::UncasedStr;
    ///
    /// let uncased_str = UncasedStr::new("Hello!");
    /// assert_eq!(uncased_str.as_str(), "Hello!");
    /// assert_ne!(uncased_str.as_str(), "hELLo!");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<I> core::ops::Index<I> for UncasedStr
where
    I: SliceIndex<str, Output = str>,
{
    type Output = UncasedStr;

    fn index(&self, index: I) -> &Self::Output {
        self.as_str()[index].into()
    }
}

impl PartialEq for UncasedStr {
    fn eq(&self, other: &UncasedStr) -> bool {
        self.0.eq_ignore_ascii_case(&other.0)
    }
}

impl PartialEq<str> for UncasedStr {
    fn eq(&self, other: &str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<UncasedStr> for str {
    fn eq(&self, other: &UncasedStr) -> bool {
        other.0.eq_ignore_ascii_case(self)
    }
}

impl PartialEq<&str> for UncasedStr {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq_ignore_ascii_case(other)
    }
}

impl PartialEq<UncasedStr> for &str {
    fn eq(&self, other: &UncasedStr) -> bool {
        other.0.eq_ignore_ascii_case(self)
    }
}

impl<'a> From<&'a str> for &'a UncasedStr {
    fn from(string: &'a str) -> &'a UncasedStr {
        UncasedStr::new(string)
    }
}

impl Eq for UncasedStr {}

impl Hash for UncasedStr {
    fn hash<H>(&self, hasher: &mut H)
    where
        H: Hasher,
    {
        self.0
            .bytes()
            .for_each(|b| hasher.write_u8(b.to_ascii_lowercase()));
    }
}

impl PartialOrd for UncasedStr {
    fn partial_cmp(&self, other: &UncasedStr) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for UncasedStr {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_chars = self.0.chars().map(|c| c.to_ascii_lowercase());
        let other_chars = other.0.chars().map(|c| c.to_ascii_lowercase());
        self_chars.cmp(other_chars)
    }
}

impl fmt::Display for UncasedStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PhfHash for UncasedStr {
    #[inline]
    fn phf_hash<H: Hasher>(&self, state: &mut H) {
        self.hash(state)
    }
}

impl FmtConst for UncasedStr {
    fn fmt_const(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // transmute is not stable in const fns (rust-lang/rust#53605), so
        // `UncasedStr::new` can't be a const fn itself, but we can inline the
        // call to transmute here in the meantime.
        f.write_str("#[allow(clippy::transmute_ptr_to_ptr)] ")?;
        f.write_str(
            "unsafe { ::std::mem::transmute::<&'static str, &'static ::ore::ascii::UncasedStr>(",
        )?;
        self.as_str().fmt_const(f)?;
        f.write_str(") }")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    macro_rules! assert_uncased_eq {
        ($($string:expr),+) => ({
            let strings = [$($string),+];

            for i in 0..strings.len() {
                for j in i..strings.len() {
                    let (str_a, str_b) = (strings[i], strings[j]);
                    let ascii_a = UncasedStr::new(str_a);
                    let ascii_b = UncasedStr::new(str_b);
                    assert_eq!(ascii_a, ascii_b);
                    assert_eq!(hash(&ascii_a), hash(&ascii_b));
                    assert_eq!(ascii_a, str_a);
                    assert_eq!(ascii_b, str_b);
                    assert_eq!(ascii_a, str_b);
                    assert_eq!(ascii_b, str_a);
                }
            }
        })
    }

    #[test]
    fn test_case_insensitive() {
        assert_uncased_eq!["a", "A"];
        assert_uncased_eq!["Aa", "aA", "AA", "aa"];
        assert_uncased_eq!["a a", "a A", "A A", "a a"];
        assert_uncased_eq!["foobar", "FOOBAR", "FooBar", "fOObAr", "fooBAR"];
        assert_uncased_eq!["", ""];
        assert_uncased_eq!["content-type", "Content-Type", "CONTENT-TYPE"];
    }

    #[test]
    fn test_case_cmp() {
        assert!(UncasedStr::new("foobar") == UncasedStr::new("FOOBAR"));
        assert!(UncasedStr::new("a") == UncasedStr::new("A"));

        assert!(UncasedStr::new("a") < UncasedStr::new("B"));
        assert!(UncasedStr::new("A") < UncasedStr::new("B"));
        assert!(UncasedStr::new("A") < UncasedStr::new("b"));

        assert!(UncasedStr::new("aa") > UncasedStr::new("a"));
        assert!(UncasedStr::new("aa") > UncasedStr::new("A"));
        assert!(UncasedStr::new("AA") > UncasedStr::new("a"));
        assert!(UncasedStr::new("AA") > UncasedStr::new("a"));
        assert!(UncasedStr::new("Aa") > UncasedStr::new("a"));
        assert!(UncasedStr::new("Aa") > UncasedStr::new("A"));
        assert!(UncasedStr::new("aA") > UncasedStr::new("a"));
        assert!(UncasedStr::new("aA") > UncasedStr::new("A"));
    }
}
