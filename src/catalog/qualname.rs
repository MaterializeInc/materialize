// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::Peekable;
use std::str::FromStr;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sql_parser::ast::{Ident, ObjectName};

pub type Result<T> = std::result::Result<T, Error>;

pub type Error = failure::Error;

/// A generalized name that may be qualified
///
/// This is something that has multiple parts separated by dots, but should have
/// comparison semantics.
///
/// # Semantic Equality
///
/// This implements equals and hash in terms of the ident *values*, not
/// including quotes.
///
/// One of the common use cases is dealing with [`sqlparser::ast::ObjectName`]s correctly
///
/// ```
/// use std::convert::TryFrom;
/// use sql_parser::ast::{Ident, ObjectName};
/// use catalog::QualName;
///
/// let with = Ident::with_quote('"', "one");
/// let without = Ident::new("two");
///
/// let on = ObjectName(vec![with, without]);
/// assert_eq!(on.to_string(), "\"one\".two");
///
/// let quoted_qn: QualName = "\"one\".two".parse().unwrap();
///
/// // Everytyhing should be equivalent to this
/// let expected: QualName = "one.two".parse().unwrap();
///
/// assert_eq!(QualName::try_from(on).unwrap(), expected);
/// assert_eq!(quoted_qn, expected);
/// ```
#[derive(Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct QualName(Vec<Identifier>);

impl QualName {
    /// Create an object name where all unquoted values are lowercased
    ///
    /// # Example
    ///
    /// ```
    /// use catalog::QualName;
    /// use sql_parser::ast::Ident;
    ///
    /// let (one, two) = (Ident::new("HoWdY"), Ident::with_quote('"', "wOwZeR"));
    /// assert_eq!(
    ///     QualName::new_normalized(vec![one, two]).unwrap().to_string(),
    ///     "howdy.\"wOwZeR\""
    /// );
    /// ```
    ///
    /// # Errors
    ///
    /// If the idents vec is empty or any ident is empty
    pub fn new_normalized<I>(idents: I) -> Result<QualName>
    where
        I: IntoIterator<Item = Ident>,
    {
        let new_idents = idents
            .into_iter()
            .map(Identifier::try_from)
            .collect::<std::result::Result<Vec<_>, _>>();
        match new_idents {
            Ok(new_idents) => {
                if !new_idents.is_empty() {
                    Ok(QualName(new_idents))
                } else {
                    failure::bail!("Tried to create a QualName with no idents")
                }
            }
            Err(_) => failure::bail!("Tried to create a QualName with at least one empty ident"),
        }
    }

    /// Create a new QualName from a list of other qualnames, in order
    ///
    /// ```
    /// # use catalog::QualName;
    /// let qn1: QualName = "one.two".parse().unwrap();
    /// let qn2: QualName = "three".parse().unwrap();
    /// let both: QualName = "one.two.three".parse().unwrap();
    ///
    /// assert_eq!(QualName::from_names(&[qn1, qn2]).unwrap(), both);
    /// ```
    pub fn from_names(names: &[QualName]) -> Result<QualName> {
        let idents: Vec<Identifier> = names.iter().flat_map(|n| &n.0).cloned().collect();
        if idents.is_empty() {
            failure::bail!("Tried to create empty qualified name from existing qualnames");
        }
        Ok(QualName(idents))
    }

    /// Get the value of the single ident in here
    ///
    /// Returns `Err` if there is more than one element in this `QualName`
    pub fn as_ident_str(&self) -> Result<&str> {
        if self.0.len() == 1 {
            Ok(&self.0[0].value)
        } else {
            failure::bail!("too many elements to possibly be an ident: {}", self)
        }
    }

    /// Create a QualName based on a string that is trusted to be correct
    pub fn trusted(ident: &str) -> QualName {
        QualName(vec![Identifier {
            value: ident.into(),
            // If it's a literal we want to use exactly what was typed
            quoted: true,
        }])
    }

    /// Compare the Object on the left with the literal string on the right
    ///
    /// This compares the Name assuming that it is not part of any namespace, the object
    /// on the right cannot be part of any namespace, if you want that you should parse
    /// the rhs.
    ///
    /// # Example
    ///
    /// ```
    /// use catalog::QualName;
    /// use sql_parser::ast::{ObjectName, Ident};
    ///
    /// let one = Ident::new("one");
    /// assert!(QualName::name_equals(ObjectName(vec![one.clone()]), "one"));
    ///
    /// let two = Ident::new("two");
    /// assert!(QualName::name_equals(ObjectName(vec![one, two]), "one.two"));
    /// ```
    pub fn name_equals(lhs: ObjectName, rhs: &'static str) -> bool {
        QualName::try_from(lhs)
            .map(|qn| &qn == rhs)
            .unwrap_or(false)
    }

    /// Create a new qualname, with the last ident in the name suffixed with `suffix`
    ///
    /// # Example
    ///
    /// ```
    /// use catalog::QualName;
    /// let qn: QualName = "one.two".parse().unwrap();
    /// let qn2 = qn.with_trailing_string("-YOU_BRED_RAPTORS?");
    /// assert_eq!(qn.to_string(), "one.two");
    /// assert_eq!(qn2.to_string(), "one.\"two-YOU_BRED_RAPTORS?\"")
    /// ```
    ///
    /// use with caution
    pub fn with_trailing_string(&self, suffix: &str) -> QualName {
        let mut new_idents = self.0.clone();
        let last = new_idents.len() - 1;
        new_idents[last].value += suffix;
        new_idents[last].quoted = true;
        QualName(new_idents)
    }
}

impl fmt::Display for QualName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut idents = self.0.iter();
        let first = idents.next().expect("qualname to have at least one ident");
        write!(f, "{}", first)?;
        for i in idents {
            write!(f, ".{}", i)?;
        }
        Ok(())
    }
}

impl FromStr for QualName {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<QualName> {
        parse_qualname(s)
    }
}

impl From<&QualName> for QualName {
    fn from(obj: &QualName) -> QualName {
        obj.clone()
    }
}

impl TryFrom<ObjectName> for QualName {
    type Error = Error;

    fn try_from(other: ObjectName) -> Result<QualName> {
        QualName::new_normalized(other.0)
    }
}

impl TryFrom<&ObjectName> for QualName {
    type Error = Error;

    fn try_from(other: &ObjectName) -> Result<QualName> {
        QualName::new_normalized(other.0.iter().cloned())
    }
}

impl TryFrom<&mut ObjectName> for QualName {
    type Error = Error;

    fn try_from(other: &mut ObjectName) -> Result<QualName> {
        QualName::new_normalized(other.0.iter().cloned())
    }
}

impl TryFrom<Ident> for QualName {
    type Error = Error;
    fn try_from(other: Ident) -> Result<QualName> {
        QualName::new_normalized(vec![other])
    }
}

impl TryFrom<&Ident> for QualName {
    type Error = Error;
    /// TODO: a version that takes a borrowed ident
    fn try_from(other: &Ident) -> Result<QualName> {
        QualName::new_normalized(vec![other.clone()])
    }
}

impl TryFrom<&str> for QualName {
    type Error = Error;
    /// An alias for [`FromStr`] for use in a generic context
    fn try_from(s: &str) -> Result<QualName> {
        s.parse()
    }
}

impl TryFrom<QualName> for Ident {
    type Error = Error;
    fn try_from(other: QualName) -> Result<Ident> {
        if other.0.len() == 1 {
            let ident = other.0.into_iter().next().unwrap();
            Ok(Ident {
                value: ident.value,
                quote_style: Some('"'),
            })
        } else {
            failure::bail!(
                "tried to create ident from qualname with {} parts: {}",
                other.0.len(),
                other
            );
        }
    }
}

impl PartialEq for QualName {
    fn eq(&self, rhs: &QualName) -> bool {
        if self.0.len() != rhs.0.len() {
            return false;
        }

        self.0.iter().zip(rhs.0.iter()).all(|(l, r)| l == r)
    }
}

impl Hash for QualName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for ident in &self.0 {
            state.write(ident.value.as_bytes());
        }
    }
}

impl PartialEq<str> for QualName {
    /// Compare QualName to a slice
    ///
    /// # Examples
    ///
    /// ```
    /// # use catalog::QualName;
    /// let qn: &QualName = &"one.TWO".parse().unwrap();
    ///
    /// assert_eq!(qn, "one.\"two\"");
    /// assert!(qn != "one.twoo");
    ///
    /// // there is one known limitation:
    /// let qn: &QualName = &"one.two.three".parse().unwrap();
    /// let s = "one.\"two.three\"";
    /// assert_eq!(qn, s);
    /// ```
    fn eq(&self, rhs: &str) -> bool {
        let mut in_quote = false;
        let mut left_chars = self
            .0
            .iter()
            .map(|ident| ident.value.as_ref())
            .intersperse(".")
            .flat_map(|val| val.chars());
        let mut right_chars = rhs.chars();
        loop {
            match (left_chars.next(), right_chars.next()) {
                (Some(left), Some(mut right)) => {
                    if right == '"' {
                        in_quote = !in_quote;
                        match right_chars.next() {
                            Some(chr) => right = chr,
                            // there is no right char after the final left
                            None => return false,
                        }
                    }

                    if in_quote {
                        if left != right {
                            return false;
                        }
                    } else {
                        for part in right.to_lowercase() {
                            if left != part {
                                return false;
                            }
                        }
                    }
                }
                (None, None) => return true,
                (None, Some('"')) if in_quote => in_quote = !in_quote,
                _ => return false,
            }
        }
    }
}

pub trait LiteralName {
    /// Convert a literal into a [`QualName`]
    fn lit(&self) -> QualName;
}

impl LiteralName for &str {
    fn lit(&self) -> QualName {
        QualName::from_str(self).expect("A valid qualified name should be provided")
    }
}

impl LiteralName for String {
    fn lit(&self) -> QualName {
        QualName::from_str(self).expect("A valid qualified name should be provided")
    }
}

///////////////////////////////////////////////////////////////////////////////
// Identifier

#[derive(Debug, Clone, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Identifier {
    value: String,
    /// True if this value needs to appear in quotes
    ///
    /// That should mean that it contains special characters,
    quoted: bool,
}

impl PartialEq for Identifier {
    fn eq(&self, rhs: &Identifier) -> bool {
        self.value == rhs.value
    }
}

impl TryFrom<Ident> for Identifier {
    type Error = Error;

    /// Construct a valid identifier
    ///
    /// Disallows empty Identifiers and normalizes case
    fn try_from(ident: Ident) -> Result<Identifier> {
        if ident.value.is_empty() {
            failure::bail!("tried to construct an empty identifier");
        }
        Ok(Identifier {
            value: if ident.quote_style.is_some() {
                ident.value
            } else {
                ident.value.to_lowercase()
            },
            quoted: ident.quote_style.is_some(),
        })
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::fmt::Write;
        if self.quoted {
            f.write_char('"')?;
        }
        f.write_str(&self.value)?;
        if self.quoted {
            f.write_char('"')?;
        }
        Ok(())
    }
}

// TODO(bwm): if these two parse functions returned iterators of NameTokens instead of
// constructed objects it would be pretty trivial to construct normalizing identity
// functions and QualNameRef/IdentifierRef types.
fn parse_qualname(s: &str) -> Result<QualName> {
    if s.is_empty() {
        failure::bail!("attempted to convert an an empty string to an identifier");
    }

    let mut idents = vec![];
    let mut chars = s.chars().peekable();
    let mut in_ident = false;
    while chars.peek().is_some() {
        in_ident = false;
        idents.push(parse_ident(&mut chars)?);
        if chars.peek() == Some(&'.') {
            chars.next();
            in_ident = true;
        }
    }
    let qn = QualName(idents);
    if in_ident {
        failure::bail!("ended qualname with a dot: {}.", qn);
    }
    Ok(qn)
}

fn parse_ident<I>(chars: &mut Peekable<I>) -> Result<Identifier>
where
    I: Iterator<Item = char>,
{
    let quoted = if chars.peek() == Some(&'"') {
        chars.next();
        true
    } else {
        false
    };
    let mut in_quote = quoted;
    let mut out = String::new();
    // this while-let satisfies lifetimes for peekable but `for..in` does not
    while let Some(chr) = chars.peek() {
        match chr {
            '"' => {
                chars.next();
                let next = chars.peek();
                if next != None && next != Some(&'.') {
                    failure::bail!(
                        "Quote character in middle of ident: {}\"{:?} (quoted: {})",
                        out,
                        next.unwrap(),
                        in_quote
                    );
                } else if !quoted {
                    failure::bail!("found quote in unquoted ident: {}\"", out);
                } else {
                    // close the quote for validation
                    in_quote = false;
                    break;
                }
            }
            '.' => {
                if quoted {
                    out.push('.');
                } else {
                    // don't consume the dot
                    break;
                }
            }
            c => {
                if quoted {
                    out.push(*c);
                } else {
                    for chr in c.to_lowercase() {
                        out.push(chr);
                    }
                }
            }
        }
        chars.next();
    }

    if in_quote {
        failure::bail!("quoted identifier does not close quote: {}", out);
    } else if out.is_empty() {
        failure::bail!("tried to create empty identifier");
    } else {
        Ok(Identifier { value: out, quoted })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_identifier_successes() {
        let legal_idents = &[
            ("one", "one", false),
            ("one.", "one", false),
            ("oNe", "one", false),
            ("\"one\"", "one", true),
            ("\"oNe\"", "oNe", true),
        ];

        for (input, value, quoted) in legal_idents {
            println!("input={} expected={} quoted={}", input, value, quoted);
            let mut chrs = input.chars().peekable();
            let out = parse_ident(&mut chrs).unwrap();
            assert_eq!(
                out,
                Identifier {
                    value: (*value).to_owned(),
                    quoted: *quoted
                }
            )
        }
    }

    #[test]
    fn parse_identifier_failures() {
        let illegal_idents = &["o\"", ""];
        for ident in illegal_idents {
            let mut chrs = ident.chars().peekable();
            assert!(
                parse_ident(&mut chrs).is_err(),
                "should be error: '{}'",
                ident
            );
        }
    }

    #[test]
    fn parse_qualname_success() {
        let legal_names = &[
            ("one.one", "one.one"),
            ("oNe.OnE", "one.one"),
            ("\"one\".one", "\"one\".one"),
            ("\"oNe\".OnE", "\"oNe\".one"),
        ];

        for (input, expected) in legal_names {
            let name = QualName::from_str(input).unwrap();
            assert_eq!(&name.to_string(), expected)
        }
    }

    #[test]
    fn parse_qualname_failures() {
        let illegal_names = &["", ".one", "oN\"e.", "o.", r#"oN\"""#];

        for input in illegal_names {
            assert!(
                &QualName::from_str(input).is_err(),
                "should be illegal qualname: {:?}",
                input
            )
        }
    }
}
