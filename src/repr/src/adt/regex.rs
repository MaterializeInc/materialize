// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regular expressions.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use proptest::prelude::any;
use proptest::prop_compose;
use regex::{Error, RegexBuilder};
use serde::de::Error as DeError;
use serde::ser::SerializeStruct;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.regex.rs"));

/// A hashable, comparable, and serializable regular expression type.
///
/// The  [`regex::Regex`] type, the de facto standard regex type in Rust, does
/// not implement [`PartialOrd`], [`Ord`] [`PartialEq`], [`Eq`], or [`Hash`].
/// The omissions are reasonable. There is no natural definition of ordering for
/// regexes. There *is* a natural definition of equality—whether two regexes
/// describe the same regular language—but that is an expensive property to
/// compute, and [`PartialEq`] is generally expected to be fast to compute.
///
/// This type wraps [`regex::Regex`] and imbues it with implementations of the
/// above traits. Two regexes are considered equal iff their string
/// representation is identical. The [`PartialOrd`], [`Ord`], and [`Hash`]
/// implementations are similarly based upon the string representation. As
/// mentioned above, is not the natural equivalence relation for regexes: for
/// example, the regexes `aa*` and `a+` define the same language, but would not
/// compare as equal with this implementation of [`PartialEq`]. Still, it is
/// often useful to have _some_ equivalence relation available (e.g., to store
/// types containing regexes in a hashmap) even if the equivalence relation is
/// imperfect.
///
/// [regex::Regex] is hard to serialize (because of the compiled code), so our approach is to
/// instead serialize this wrapper struct, where we skip serializing the actual regex field, and
/// we reconstruct the regex field from the other two fields upon deserialization.
/// (Earlier, serialization was buggy due to <https://github.com/tailhook/serde-regex/issues/14>,
/// and also making the same mistake in our own protobuf serialization code.)
#[derive(Debug, Clone, MzReflect)]
pub struct Regex {
    pub pattern: String,
    pub case_insensitive: bool,
    pub regex: regex::Regex,
}

impl Regex {
    pub fn new(pattern: String, case_insensitive: bool) -> Result<Regex, Error> {
        let mut regex_builder = RegexBuilder::new(pattern.as_str());
        regex_builder.case_insensitive(case_insensitive);
        Ok(Regex {
            pattern,
            case_insensitive,
            regex: regex_builder.build()?,
        })
    }
}

impl PartialEq<Regex> for Regex {
    fn eq(&self, other: &Regex) -> bool {
        self.pattern == other.pattern && self.case_insensitive == other.case_insensitive
    }
}

impl Eq for Regex {}

impl PartialOrd for Regex {
    fn partial_cmp(&self, other: &Regex) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Regex {
    fn cmp(&self, other: &Regex) -> Ordering {
        (&self.pattern, self.case_insensitive).cmp(&(&other.pattern, other.case_insensitive))
    }
}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.pattern.hash(hasher);
        self.case_insensitive.hash(hasher);
    }
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.regex
    }
}

impl RustType<ProtoRegex> for Regex {
    fn into_proto(&self) -> ProtoRegex {
        ProtoRegex {
            pattern: self.pattern.clone(),
            case_insensitive: self.case_insensitive,
        }
    }

    fn from_proto(proto: ProtoRegex) -> Result<Self, TryFromProtoError> {
        Ok(Regex::new(proto.pattern, proto.case_insensitive)?)
    }
}

impl Serialize for Regex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Regex", 2)?;
        state.serialize_field("pattern", &self.pattern)?;
        state.serialize_field("case_insensitive", &self.case_insensitive)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Regex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Pattern,
            CaseInsensitive,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("pattern string or case_insensitive bool")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "pattern" => Ok(Field::Pattern),
                            "case_insensitive" => Ok(Field::CaseInsensitive),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct RegexVisitor;

        impl<'de> de::Visitor<'de> for RegexVisitor {
            type Value = Regex;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Regex serialized by the manual Serialize impl from above")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Regex, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let pattern = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let case_insensitive = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Regex::new(pattern, case_insensitive).map_err(|err| {
                    V::Error::custom(format!(
                        "Unable to recreate regex during deserialization: {}",
                        err
                    ))
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Regex, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut pattern: Option<String> = None;
                let mut case_insensitive: Option<bool> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Pattern => {
                            if pattern.is_some() {
                                return Err(de::Error::duplicate_field("pattern"));
                            }
                            pattern = Some(map.next_value()?);
                        }
                        Field::CaseInsensitive => {
                            if case_insensitive.is_some() {
                                return Err(de::Error::duplicate_field("case_insensitive"));
                            }
                            case_insensitive = Some(map.next_value()?);
                        }
                    }
                }
                let pattern = pattern.ok_or_else(|| de::Error::missing_field("pattern"))?;
                let case_insensitive =
                    case_insensitive.ok_or_else(|| de::Error::missing_field("case_insensitive"))?;
                Regex::new(pattern, case_insensitive).map_err(|err| {
                    V::Error::custom(format!(
                        "Unable to recreate regex during deserialization: {}",
                        err
                    ))
                })
            }
        }

        const FIELDS: &[&str] = &["pattern", "case_insensitive"];
        deserializer.deserialize_struct("Regex", FIELDS, RegexVisitor)
    }
}

// TODO: this is not really high priority, but this could modified to generate a
// greater variety of regexes. Ignoring the beginning-of-file/line and EOF/EOL
// symbols, the only regexes being generated are `.{#repetitions}` and
// `x{#repetitions}`.
const BEGINNING_SYMBOLS: &str = r"((\\A)|\^)?";
const CHARACTERS: &str = r"[\.x]{1}";
const REPETITIONS: &str = r"((\*|\+|\?|(\{[1-9],?\}))\??)?";
const END_SYMBOLS: &str = r"(\$|(\\z))?";

prop_compose! {
    pub fn any_regex()
                (b in BEGINNING_SYMBOLS, c in CHARACTERS,
                 r in REPETITIONS, e in END_SYMBOLS, case_insensitive in any::<bool>())
                -> Regex {
        let string = format!("{}{}{}{}", b, c, r, e);
        Regex::new(string, case_insensitive).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn regex_protobuf_roundtrip( expect in any_regex() ) {
            let actual =  protobuf_roundtrip::<_, ProtoRegex>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    // This was failing before due to the derived serde serialization being incorrect, because of
    // <https://github.com/tailhook/serde-regex/issues/14>.
    // Nowadays, we use our own handwritten Serialize/Deserialize impls for our Regex wrapper struct.
    #[mz_ore::test]
    fn regex_serde_case_insensitive() {
        let orig_regex = Regex::new("AAA".to_string(), true).unwrap();
        let serialized: String = serde_json::to_string(&orig_regex).unwrap();
        let roundtrip_result: Regex = serde_json::from_str(&serialized).unwrap();
        // Equality test between orig and roundtrip_result wouldn't work, because Eq doesn't test
        // the actual regex object. So test the actual regex functionality (concentrating on case
        // sensitivity).
        assert_eq!(orig_regex.regex.is_match("aaa"), true);
        assert_eq!(roundtrip_result.regex.is_match("aaa"), true);
    }
}
