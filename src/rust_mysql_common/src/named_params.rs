// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::borrow::Cow;

/// Appears if a statement have both named and positional parameters.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct MixedParamsError;

enum ParserState {
    TopLevel,
    // (string_delimiter, last_char)
    InStringLiteral(u8, u8),
    MaybeInNamedParam,
    InNamedParam,
    InSharpComment,
    MaybeInDoubleDashComment1,
    MaybeInDoubleDashComment2,
    InDoubleDashComment,
    MaybeInCComment1,
    MaybeInCComment2,
    InCComment,
    MaybeExitCComment,
    InQuotedIdentifier,
}

use self::ParserState::*;

/// Parsed named params (see [`ParsedNamedParams::parse`]).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ParsedNamedParams<'a> {
    query: Cow<'a, [u8]>,
    params: Vec<Cow<'a, [u8]>>,
}

impl<'a> ParsedNamedParams<'a> {
    /// Parse named params in the given query.
    ///
    /// Parameters must be named according to the following convention:
    ///
    /// * parameter name must start with either `_` or `a..z`
    /// * parameter name may continue with `_`, `a..z` and `0..9`
    pub fn parse(query: &'a [u8]) -> Result<Self, MixedParamsError> {
        let mut state = TopLevel;
        let mut have_positional = false;
        let mut cur_param = 0;
        // Vec<(colon_offset, start_offset, end_offset)>
        let mut params = Vec::new();
        for (i, c) in query.iter().enumerate() {
            let mut rematch = false;
            match state {
                TopLevel => match c {
                    b':' => state = MaybeInNamedParam,
                    b'/' => state = MaybeInCComment1,
                    b'-' => state = MaybeInDoubleDashComment1,
                    b'#' => state = InSharpComment,
                    b'\'' => state = InStringLiteral(b'\'', b'\''),
                    b'"' => state = InStringLiteral(b'"', b'"'),
                    b'?' => have_positional = true,
                    b'`' => state = InQuotedIdentifier,
                    _ => (),
                },
                InStringLiteral(separator, prev_char) => match c {
                    x if *x == separator && prev_char != b'\\' => state = TopLevel,
                    x => state = InStringLiteral(separator, *x),
                },
                MaybeInNamedParam => match c {
                    b'a'..=b'z' | b'_' => {
                        params.push((i - 1, i, 0));
                        state = InNamedParam;
                    }
                    _ => rematch = true,
                },
                InNamedParam => {
                    if !matches!(c, b'a'..=b'z' | b'0'..=b'9' | b'_') {
                        params[cur_param].2 = i;
                        cur_param += 1;
                        rematch = true;
                    }
                }
                InSharpComment => {
                    if *c == b'\n' {
                        state = TopLevel
                    }
                }
                MaybeInDoubleDashComment1 => match c {
                    b'-' => state = MaybeInDoubleDashComment2,
                    _ => state = TopLevel,
                },
                MaybeInDoubleDashComment2 => {
                    if c.is_ascii_whitespace() && *c != b'\n' {
                        state = InDoubleDashComment
                    } else {
                        state = TopLevel
                    }
                }
                InDoubleDashComment => {
                    if *c == b'\n' {
                        state = TopLevel
                    }
                }
                MaybeInCComment1 => match c {
                    b'*' => state = MaybeInCComment2,
                    _ => state = TopLevel,
                },
                MaybeInCComment2 => match c {
                    b'!' | b'+' => state = TopLevel, // extensions and optimizer hints
                    _ => state = InCComment,
                },
                InCComment => {
                    if *c == b'*' {
                        state = MaybeExitCComment
                    }
                }
                MaybeExitCComment => match c {
                    b'/' => state = TopLevel,
                    _ => state = InCComment,
                },
                InQuotedIdentifier => {
                    if *c == b'`' {
                        state = TopLevel
                    }
                }
            }
            if rematch {
                match c {
                    b':' => state = MaybeInNamedParam,
                    b'\'' => state = InStringLiteral(b'\'', b'\''),
                    b'"' => state = InStringLiteral(b'"', b'"'),
                    _ => state = TopLevel,
                }
            }
        }

        if let InNamedParam = state {
            params[cur_param].2 = query.len();
        }

        if !params.is_empty() {
            if have_positional {
                return Err(MixedParamsError);
            }
            let mut real_query = Vec::with_capacity(query.len());
            let mut last = 0;
            let mut out_params = Vec::with_capacity(params.len());
            for (colon_offset, start, end) in params {
                real_query.extend(&query[last..colon_offset]);
                real_query.push(b'?');
                last = end;
                out_params.push(Cow::Borrowed(&query[start..end]));
            }
            real_query.extend(&query[last..]);
            Ok(Self {
                query: Cow::Owned(real_query),
                params: out_params,
            })
        } else {
            Ok(Self {
                query: Cow::Borrowed(query),
                params: vec![],
            })
        }
    }

    /// Returns a query string to pass to MySql (named parameters have been replaced with `?`).
    pub fn query(&self) -> &[u8] {
        &self.query
    }

    /// Names of named parameters in order of appearance.
    ///
    /// # Note
    ///
    /// * the returned slice might be empty if original query contained
    ///   no named parameters.
    /// * same name may appear multiple times.
    pub fn params(&self) -> &[Cow<'a, [u8]>] {
        &self.params
    }
}

#[cfg(test)]
mod test {
    use super::*;

    macro_rules! cows {
        ($($l:expr),+ $(,)?) => { &[$(Cow::Borrowed(&$l[..]),)*] };
    }

    #[test]
    fn should_parse_named_params() {
        let result = ParsedNamedParams::parse(b":a :b").unwrap();
        assert_eq!(result.query(), b"? ?");
        assert_eq!(result.params(), cows!(b"a", b"b"));

        let result = ParsedNamedParams::parse(b"SELECT (:a-10)").unwrap();
        assert_eq!(result.query(), b"SELECT (?-10)");
        assert_eq!(result.params(), cows!(b"a"));

        let result = ParsedNamedParams::parse(br#"SELECT '"\':a' "'\"':c" :b"#).unwrap();
        assert_eq!(result.query(), br#"SELECT '"\':a' "'\"':c" ?"#);
        assert_eq!(result.params(), cows!(b"b"));

        let result = ParsedNamedParams::parse(br":a_Aa:b").unwrap();
        assert_eq!(result.query(), b"?Aa?");
        assert_eq!(result.params(), cows!(b"a_", b"b"));

        let result = ParsedNamedParams::parse(br"::b").unwrap();
        assert_eq!(result.query(), b":?");
        assert_eq!(result.params(), cows!(b"b"));

        ParsedNamedParams::parse(b":a ?").unwrap_err();
    }

    #[test]
    fn should_allow_numbers_in_param_name() {
        let result = ParsedNamedParams::parse(b":a1 :a2").unwrap();
        assert_eq!(result.query(), b"? ?");
        assert_eq!(result.params(), cows!(b"a1", b"a2"));

        let result = ParsedNamedParams::parse(b":1a :2a").unwrap();
        assert_eq!(result.query(), b":1a :2a");
        assert!(result.params().is_empty());
    }

    #[test]
    fn special_characters_in_query() {
        let result =
            ParsedNamedParams::parse("SELECT 1 FROM été WHERE thing = :param;".as_bytes()).unwrap();
        assert_eq!(
            result.query(),
            "SELECT 1 FROM été WHERE thing = ?;".as_bytes()
        );
        assert_eq!(result.params(), cows!(b"param"));
    }

    #[test]
    fn comments_with_question_marks() {
        let result = ParsedNamedParams::parse(
            "SELECT 1 FROM my_table WHERE thing = :param;/* question\n  mark '?' in multiline\n\
            comment? */\n# ??- sharp comment -??\n-- dash-dash?\n/*! extention param :param2 */\n\
            /*+ optimizer hint :param3 */; select :foo; # another comment?"
                .as_bytes(),
        )
        .unwrap();
        assert_eq!(
            result.query(),
            b"SELECT 1 FROM my_table WHERE thing = ?;/* question\n  mark '?' in multiline\n\
        comment? */\n# ??- sharp comment -??\n-- dash-dash?\n/*! extention param ? */\n\
        /*+ optimizer hint ? */; select ?; # another comment?"
        );
        assert_eq!(
            result.params(),
            cows!(b"param", b"param2", b"param3", b"foo"),
        );
    }

    #[test]
    fn quoted_identifier() {
        let result = ParsedNamedParams::parse(b"INSERT INTO `my:table` VALUES (?)").unwrap();
        assert_eq!(result.query(), b"INSERT INTO `my:table` VALUES (?)");
        assert!(result.params().is_empty());

        let result = ParsedNamedParams::parse(b"INSERT INTO `my:table` VALUES (:foo)").unwrap();
        assert_eq!(result.query(), b"INSERT INTO `my:table` VALUES (?)");
        assert_eq!(result.params(), cows!(b"foo"));
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use super::*;

        #[bench]
        fn parse_ten_named_params(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let result = ParsedNamedParams::parse(
                    r#"
                SELECT :one, :two, :three, :four, :five, :six, :seven, :eight, :nine, :ten
                "#,
                )
                .unwrap();
                test::black_box(result);
            });
        }

        #[bench]
        fn parse_zero_named_params(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let result = ParsedNamedParams::parse(
                    r"
                SELECT one, two, three, four, five, six, seven, eight, nine, ten
                ",
                )
                .unwrap();
                test::black_box(result);
            });
        }
    }
}
