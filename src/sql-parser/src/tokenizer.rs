// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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

//! SQL Tokenizer
//!
//! The tokenizer (a.k.a. lexer) converts a string into a sequence of tokens.
//!
//! The tokens then form the input for the parser, which outputs an Abstract Syntax Tree (AST).

use std::convert::TryFrom;
use std::fmt;
use std::ops::Range;

use crate::keywords::ALL_KEYWORDS;
use crate::parser::ParserError;

/// SQL Token enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A keyword (like SELECT) or an optionally quoted SQL identifier
    Word(Word),
    /// An unsigned numeric literal
    Number(String),
    /// A character that could not be tokenized
    Char(char),
    /// Single quoted string: i.e: 'string'
    String(String),
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexString(String),
    /// An unsigned numeric literal representing positional
    /// parameters like $1, $2, etc. in prepared statements and
    /// function definitions
    Parameter(String),
    /// Comma
    Comma,
    /// Whitespace (space, tab, etc)
    Whitespace(Whitespace),
    /// Equality operator `=`
    Eq,
    /// Not Equals operator `<>` (or `!=` in some dialects)
    Neq,
    /// Less Than operator `<`
    Lt,
    /// Greater han operator `>`
    Gt,
    /// Less Than Or Equals operator `<=`
    LtEq,
    /// Greater Than Or Equals operator `>=`
    GtEq,
    /// Plus operator `+`
    Plus,
    /// Minus operator `-`
    Minus,
    /// Multiplication operator `*`
    Mult,
    /// Division operator `/`
    Div,
    /// Modulo Operator `%`
    Mod,
    /// Concat operator '||'
    Concat,
    /// Regex match operator `~`
    RegexMatch,
    /// Regex case-insensitive match operator `~*`
    RegexIMatch,
    /// Regex does-not-match operator `!~`
    RegexNotMatch,
    /// Regex case-insensitive does-not-match operator `!~*`
    RegexNotIMatch,
    // Json functions are documented at https://www.postgresql.org/docs/current/functions-json.html
    /// Get json field operator '->'
    JsonGet,
    /// Get json field as text operator '->>'
    JsonGetAsText,
    /// Get json path operator '#>'
    JsonGetPath,
    /// Get json path as text operator '#>>'
    JsonGetPathAsText,
    /// Json contains json operator '@>'
    JsonContainsJson,
    /// Json contained-in json operator '<@'
    JsonContainedInJson,
    /// Json contains field operator '?'
    JsonContainsField,
    /// Json contains any fields operator '?|'
    JsonContainsAnyFields,
    /// Json contains any fields operator '?&'
    JsonContainsAllFields,
    /// Json delete path operator '#-'
    JsonDeletePath,
    /// Json contains path operator '@?'
    JsonContainsPath,
    /// Json apply path predicate operator '@@'
    JsonApplyPathPredicate,
    /// Left parenthesis `(`
    LParen,
    /// Right parenthesis `)`
    RParen,
    /// Period (used for compound identifiers or projections into nested types)
    Period,
    /// Colon `:`
    Colon,
    /// DoubleColon `::` (used for casting in postgresql)
    DoubleColon,
    /// SemiColon `;` used as separator for COPY and payload
    SemiColon,
    /// Backslash `\` used in terminating the COPY payload with `\.`
    Backslash,
    /// Left bracket `[`
    LBracket,
    /// Right bracket `]`
    RBracket,
    /// Ampersand &
    Ampersand,
    /// Left brace `{`
    LBrace,
    /// Right brace `}`
    RBrace,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Word(ref w) => write!(f, "{}", w),
            Token::Number(ref n) => f.write_str(n),
            Token::Char(ref c) => write!(f, "{}", c),
            Token::String(ref s) => write!(f, "'{}'", s),
            Token::HexString(ref s) => write!(f, "X'{}'", s),
            Token::Parameter(n) => write!(f, "${}", n),
            Token::Comma => f.write_str(","),
            Token::Whitespace(ws) => write!(f, "{}", ws),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("<>"),
            Token::Lt => f.write_str("<"),
            Token::Gt => f.write_str(">"),
            Token::LtEq => f.write_str("<="),
            Token::GtEq => f.write_str(">="),
            Token::Plus => f.write_str("+"),
            Token::Minus => f.write_str("-"),
            Token::Mult => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::Mod => f.write_str("%"),
            Token::Concat => f.write_str("||"),
            Token::RegexMatch => f.write_str("~"),
            Token::RegexIMatch => f.write_str("~*"),
            Token::RegexNotMatch => f.write_str("!~"),
            Token::RegexNotIMatch => f.write_str("!~*"),
            Token::JsonGet => f.write_str("->"),
            Token::JsonGetAsText => f.write_str("->>"),
            Token::JsonGetPath => f.write_str("#>"),
            Token::JsonGetPathAsText => f.write_str("#>>"),
            Token::JsonContainsJson => f.write_str("@>"),
            Token::JsonContainedInJson => f.write_str("<@"),
            Token::JsonContainsField => f.write_str("?"),
            Token::JsonContainsAnyFields => f.write_str("?|"),
            Token::JsonContainsAllFields => f.write_str("?&"),
            Token::JsonDeletePath => f.write_str("#-"),
            Token::JsonContainsPath => f.write_str("@?"),
            Token::JsonApplyPathPredicate => f.write_str("@@"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::Period => f.write_str("."),
            Token::Colon => f.write_str(":"),
            Token::DoubleColon => f.write_str("::"),
            Token::SemiColon => f.write_str(";"),
            Token::Backslash => f.write_str("\\"),
            Token::LBracket => f.write_str("["),
            Token::RBracket => f.write_str("]"),
            Token::Ampersand => f.write_str("&"),
            Token::LBrace => f.write_str("{"),
            Token::RBrace => f.write_str("}"),
        }
    }
}

impl Token {
    #[cfg(test)]
    pub fn make_keyword(keyword: &str) -> Self {
        Token::make_word(keyword, None)
    }

    pub fn make_word(word: &str, quote_style: Option<char>) -> Self {
        let word_uppercase = word.to_uppercase();
        //TODO: need to reintroduce FnvHashSet at some point .. iterating over keywords is
        // not fast but I want the simplicity for now while I experiment with pluggable
        // dialects
        let is_keyword = quote_style == None && ALL_KEYWORDS.contains(&word_uppercase.as_str());
        Token::Word(Word {
            value: word.to_string(),
            quote_style,
            keyword: if is_keyword {
                word_uppercase
            } else {
                "".to_string()
            },
        })
    }
}

/// A keyword (like SELECT) or an optionally quoted SQL identifier
#[derive(Debug, Clone, PartialEq)]
pub struct Word {
    /// The value of the token, without the enclosing quotes, and with the
    /// escape sequences (if any) processed (TODO: escapes are not handled)
    pub value: String,
    /// An identifier can be "quoted" (&lt;delimited identifier> in ANSI parlance).
    /// The standard and most implementations allow using double quotes for this,
    /// but some implementations support other quoting styles as well (e.g. \[MS SQL])
    pub quote_style: Option<char>,
    /// If the word was not quoted and it matched one of the known keywords,
    /// this will have one of the values from dialect::keywords, otherwise empty
    pub keyword: String,
}

impl fmt::Display for Word {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.quote_style {
            Some(s) if s == '"' || s == '[' || s == '`' => {
                write!(f, "{}{}{}", s, self.value, Word::matching_end_quote(s))
            }
            None => f.write_str(&self.value),
            _ => panic!("Unexpected quote_style!"),
        }
    }
}
impl Word {
    fn matching_end_quote(ch: char) -> char {
        match ch {
            '"' => '"', // ANSI and most dialects
            '[' => ']', // MS SQL
            '`' => '`', // MySQL
            _ => panic!("unexpected quoting style!"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Whitespace {
    Space,
    Newline,
    Tab,
    SingleLineComment(String),
    MultiLineComment(String),
}

impl fmt::Display for Whitespace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Whitespace::Space => f.write_str(" "),
            Whitespace::Newline => f.write_str("\n"),
            Whitespace::Tab => f.write_str("\t"),
            Whitespace::SingleLineComment(s) => write!(f, "--{}", s),
            Whitespace::MultiLineComment(s) => write!(f, "/*{}*/", s),
        }
    }
}

/// Like `str.chars().peekable()` except we can get at the underlying index
struct PeekableChars<'a> {
    str: &'a str,
    index: usize,
}

impl<'a> PeekableChars<'a> {
    fn next(&mut self) -> Option<char> {
        let c = self.peek();
        if let Some(c) = c {
            self.index += c.len_utf8();
        }
        c
    }

    fn next_n(&mut self, n: usize) -> Option<&'a str> {
        match self.str[self.index..].char_indices().nth(n) {
            Some((i, _)) => {
                let s = &self.str[self.index..self.index + i];
                self.index += i;
                Some(s)
            }
            None => None,
        }
    }

    fn peek(&self) -> Option<char> {
        self.str[self.index..].chars().next()
    }
}

/// SQL Tokenizer
pub struct Tokenizer {
    pub query: String,
}

impl Tokenizer {
    /// Create a new SQL tokenizer for the specified SQL statement
    pub fn new(query: &str) -> Self {
        Self {
            query: query.to_string(),
        }
    }

    /// Tokenize the statement and produce a vector of tokens
    pub fn tokenize(&mut self) -> Result<Vec<(Token, Range<usize>)>, ParserError> {
        let mut peekable = PeekableChars {
            str: &self.query,
            index: 0,
        };

        let mut tokens: Vec<(Token, Range<usize>)> = vec![];

        loop {
            let start = peekable.index;
            let token = self.next_token(&mut peekable);
            let end = peekable.index;
            match token {
                Err(message) => {
                    return Err(ParserError {
                        sql: self.query.clone(),
                        range: start..end,
                        message,
                    })
                }
                Ok(Some(token)) => tokens.push((token, start..end)),
                Ok(None) => break,
            }
        }
        Ok(tokens)
    }

    /// Get the next token or return None
    fn next_token(&self, chars: &mut PeekableChars) -> Result<Option<Token>, String> {
        //println!("next_token: {:?}", chars.peek());
        match chars.peek() {
            Some(ch) => match ch {
                ' ' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Space)),
                '\t' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Tab)),
                '\n' => self.consume_and_return(chars, Token::Whitespace(Whitespace::Newline)),
                '\r' => {
                    // Emit a single Whitespace::Newline token for \r and \r\n
                    chars.next();
                    if let Some('\n') = chars.peek() {
                        chars.next();
                    }
                    Ok(Some(Token::Whitespace(Whitespace::Newline)))
                }
                x @ 'x' | x @ 'X' => {
                    chars.next();
                    match chars.peek() {
                        Some('\'') => {
                            // Hex string literal.
                            let s = self.tokenize_string(chars)?;
                            Ok(Some(Token::HexString(s)))
                        }
                        _ => {
                            // Regular identifier starting with an "X".
                            let s = self.tokenize_word(x, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                e @ 'e' | e @ 'E' => {
                    chars.next();
                    match chars.peek() {
                        Some('\'') => {
                            // Extended string literal.
                            let s = self.tokenize_extended_string(chars)?;
                            Ok(Some(Token::String(s)))
                        }
                        _ => {
                            // Regular identifier starting with an "E".
                            let s = self.tokenize_word(e, chars);
                            Ok(Some(Token::make_word(&s, None)))
                        }
                    }
                }
                // identifier or keyword
                ch if self.is_identifier_start(ch) => {
                    chars.next(); // consume the first char
                    let s = self.tokenize_word(ch, chars);
                    Ok(Some(Token::make_word(&s, None)))
                }
                // string
                '\'' => {
                    let s = self.tokenize_string(chars)?;
                    Ok(Some(Token::String(s)))
                }
                // delimited (quoted) identifier
                quote_start if self.is_delimited_identifier_start(quote_start) => {
                    // TODO(justin): handle doubled-up quotes: "foo""bar" should be `foo"bar`.
                    chars.next(); // consume the opening quote
                    let quote_end = Word::matching_end_quote(quote_start);
                    let s = peeking_take_while(chars, |ch| ch != quote_end);
                    if chars.next() == Some(quote_end) {
                        Ok(Some(Token::make_word(&s, Some(quote_start))))
                    } else {
                        Err(format!(
                            "Expected close delimiter '{}' before EOF.",
                            quote_end
                        ))
                    }
                }
                // numbers
                '0'..='9' => self.tokenize_number(chars, false),
                // punctuation
                '(' => self.consume_and_return(chars, Token::LParen),
                ')' => self.consume_and_return(chars, Token::RParen),
                ',' => self.consume_and_return(chars, Token::Comma),
                // operators
                '-' => {
                    chars.next(); // consume the '-'
                    match chars.peek() {
                        Some('-') => {
                            chars.next(); // consume the second '-', starting a single-line comment
                            let mut s = peeking_take_while(chars, |ch| ch != '\n');
                            if let Some(ch) = chars.next() {
                                assert_eq!(ch, '\n');
                                s.push(ch);
                            }
                            Ok(Some(Token::Whitespace(Whitespace::SingleLineComment(s))))
                        }
                        Some('>') => {
                            chars.next(); // consume the '>'
                            match chars.peek() {
                                Some('>') => self.consume_and_return(chars, Token::JsonGetAsText),
                                _ => Ok(Some(Token::JsonGet)),
                            }
                        }
                        // a regular '-' operator
                        _ => Ok(Some(Token::Minus)),
                    }
                }
                '/' => {
                    chars.next(); // consume the '/'
                    match chars.peek() {
                        Some('*') => {
                            chars.next(); // consume the '*', starting a multi-line comment
                            self.tokenize_multiline_comment(chars)
                        }
                        // a regular '/' operator
                        _ => Ok(Some(Token::Div)),
                    }
                }
                '+' => self.consume_and_return(chars, Token::Plus),
                '*' => self.consume_and_return(chars, Token::Mult),
                '%' => self.consume_and_return(chars, Token::Mod),
                '#' => {
                    chars.next(); // consume '#'
                    match chars.peek() {
                        Some('>') => {
                            chars.next(); // consume '>'
                            match chars.peek() {
                                Some('>') => {
                                    self.consume_and_return(chars, Token::JsonGetPathAsText)
                                }
                                _ => Ok(Some(Token::JsonGetPath)),
                            }
                        }
                        Some('-') => self.consume_and_return(chars, Token::JsonDeletePath),
                        _ => Err("Unrecognized token".to_string()),
                    }
                }
                '@' => {
                    chars.next(); // consume '@'
                    match chars.peek() {
                        Some('>') => self.consume_and_return(chars, Token::JsonContainsJson),
                        Some('?') => self.consume_and_return(chars, Token::JsonContainsPath),
                        Some('@') => self.consume_and_return(chars, Token::JsonApplyPathPredicate),
                        _ => Err("Unrecognized token".to_string()),
                    }
                }
                '?' => {
                    chars.next(); // consume '?'
                    match chars.peek() {
                        Some('|') => self.consume_and_return(chars, Token::JsonContainsAnyFields),
                        Some('&') => self.consume_and_return(chars, Token::JsonContainsAllFields),
                        _ => Ok(Some(Token::JsonContainsField)),
                    }
                }
                '|' => {
                    chars.next(); // consume '|'
                    match chars.peek() {
                        Some('|') => self.consume_and_return(chars, Token::Concat),
                        _ => Err("Unrecognized token".to_string()),
                    }
                }
                '~' => {
                    chars.next(); // consume '~'
                    match chars.peek() {
                        Some('*') => self.consume_and_return(chars, Token::RegexIMatch),
                        _ => Ok(Some(Token::RegexMatch)),
                    }
                }
                '=' => self.consume_and_return(chars, Token::Eq),
                '.' => {
                    chars.next(); // consume '.'
                    match chars.peek() {
                        Some('0'..='9') => self.tokenize_number(chars, true),
                        _ => Ok(Some(Token::Period)),
                    }
                }
                '!' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('~') => {
                            chars.next(); // consume '~'
                            match chars.peek() {
                                Some('*') => self.consume_and_return(chars, Token::RegexNotIMatch),
                                _ => Ok(Some(Token::RegexNotMatch)),
                            }
                        }
                        Some('=') => self.consume_and_return(chars, Token::Neq),
                        _ => Err("Unrecognized token".to_string()),
                    }
                }
                '<' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::LtEq),
                        Some('>') => self.consume_and_return(chars, Token::Neq),
                        Some('@') => self.consume_and_return(chars, Token::JsonContainedInJson),
                        _ => Ok(Some(Token::Lt)),
                    }
                }
                '>' => {
                    chars.next(); // consume
                    match chars.peek() {
                        Some('=') => self.consume_and_return(chars, Token::GtEq),
                        _ => Ok(Some(Token::Gt)),
                    }
                }
                ':' => {
                    chars.next();
                    match chars.peek() {
                        Some(':') => self.consume_and_return(chars, Token::DoubleColon),
                        _ => Ok(Some(Token::Colon)),
                    }
                }
                ';' => self.consume_and_return(chars, Token::SemiColon),
                '\\' => self.consume_and_return(chars, Token::Backslash),
                '[' => self.consume_and_return(chars, Token::LBracket),
                ']' => self.consume_and_return(chars, Token::RBracket),
                '&' => self.consume_and_return(chars, Token::Ampersand),
                '{' => self.consume_and_return(chars, Token::LBrace),
                '}' => self.consume_and_return(chars, Token::RBrace),
                '$' => self.tokenize_parameter(chars),
                other => self.consume_and_return(chars, Token::Char(other)),
            },
            None => Ok(None),
        }
    }

    /// Tokenize an identifier or keyword, after the first char is already consumed.
    fn tokenize_word(&self, first_char: char, chars: &mut PeekableChars) -> String {
        let mut s = first_char.to_string();
        s.push_str(&peeking_take_while(chars, |ch| self.is_identifier_part(ch)));
        s
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_string(&self, chars: &mut PeekableChars) -> Result<String, String> {
        let mut s = String::new();
        chars.next(); // Consume the opening quote.
        loop {
            match chars.next() {
                Some('\'') => {
                    if chars.peek() == Some('\'') {
                        // Escaped quote.
                        s.push('\'');
                        chars.next();
                    } else {
                        break Ok(s);
                    }
                }
                Some(c) => s.push(c),
                None => return Err("unexpected EOF while parsing string literal".into()),
            }
        }
    }

    /// Read a single quoted string, starting with the opening quote.
    fn tokenize_extended_string(&self, chars: &mut PeekableChars) -> Result<String, String> {
        let mut s = String::new();
        chars.next(); // Consume the opening quote.
        let unexpected_eof = || Err("unexpected EOF while parsing extended string literal".into());
        let parse_unicode_escape = |s| match s {
            Some(s) => u32::from_str_radix(s, 16)
                .ok()
                .and_then(|codepoint| char::try_from(codepoint).ok())
                .ok_or_else(|| "invalid unicode escape sequence"),
            None => Err("too few digits in unicode escape sequence"),
        };
        loop {
            match chars.next() {
                Some('\'') => {
                    if chars.peek() == Some('\'') {
                        // Escaped quote.
                        s.push('\'');
                        chars.next();
                    } else {
                        break Ok(s);
                    }
                }
                Some('\\') => match chars.next() {
                    Some('b') => s.push('\x08'),
                    Some('f') => s.push('\x12'),
                    Some('n') => s.push('\n'),
                    Some('r') => s.push('\r'),
                    Some('t') => s.push('\t'),
                    Some('u') => s.push(parse_unicode_escape(chars.next_n(4))?),
                    Some('U') => s.push(parse_unicode_escape(chars.next_n(8))?),
                    // NOTE: we do not support octal (\o) or hexadecimal (\x)
                    // escapes, since it is possible to construct invalid
                    // UTF-8 with those escapes. We could check for and reject
                    // invalid UTF-8, of course, but it is too annoying to be
                    // worth doing right now.
                    Some(c) => s.push(c),
                    None => return unexpected_eof(),
                },
                Some(c) => s.push(c),
                None => return unexpected_eof(),
            }
        }
    }

    fn tokenize_multiline_comment(
        &self,
        chars: &mut PeekableChars,
    ) -> Result<Option<Token>, String> {
        let mut s = String::new();
        let mut maybe_closing_comment = false;
        // TODO: deal with nested comments
        loop {
            match chars.next() {
                Some(ch) => {
                    if maybe_closing_comment {
                        if ch == '/' {
                            break Ok(Some(Token::Whitespace(Whitespace::MultiLineComment(s))));
                        } else {
                            s.push('*');
                        }
                    }
                    maybe_closing_comment = ch == '*';
                    if !maybe_closing_comment {
                        s.push(ch);
                    }
                }
                None => {
                    break Err("Unexpected EOF while in a multi-line comment".to_string());
                }
            }
        }
    }

    /// PostgreSQL supports positional parameters (like $1, $2, etc.) for
    /// prepared statements and function definitions.
    /// Grab the positional argument following a $ to parse it.
    fn tokenize_parameter(&self, chars: &mut PeekableChars) -> Result<Option<Token>, String> {
        assert_eq!(Some('$'), chars.next());

        let n = peeking_take_while(chars, |ch| match ch {
            '0'..='9' => true,
            _ => false,
        });

        if n.is_empty() {
            chars.next();
            return Err("parameter marker ($) was not followed by at least one digit".to_string());
        }

        Ok(Some(Token::Parameter(n)))
    }

    fn tokenize_number(
        &self,
        chars: &mut PeekableChars,
        seen_decimal: bool,
    ) -> Result<Option<Token>, String> {
        let mut seen_decimal = seen_decimal;
        let mut s = if seen_decimal {
            ".".to_string()
        } else {
            String::default()
        };

        s.push_str(&peeking_take_while(chars, |ch| match ch {
            '0'..='9' => true,
            '.' if !seen_decimal => {
                seen_decimal = true;
                true
            }
            _ => false,
        }));
        // If in e-notation, parse the e-notation with special care given to negative exponents.
        match chars.peek() {
            Some('e') | Some('E') => {
                s.push('E');
                // Consume the e-notation signifier.
                chars.next();
                if let Some('-') = chars.peek() {
                    s.push('-');
                    // Consume the negative sign.
                    chars.next();
                }
                let e = peeking_take_while(chars, |ch| match ch {
                    '0'..='9' => true,
                    _ => false,
                });
                s.push_str(&e);
            }
            _ => {}
        }

        Ok(Some(Token::Number(s)))
    }

    fn consume_and_return(
        &self,
        chars: &mut PeekableChars,
        t: Token,
    ) -> Result<Option<Token>, String> {
        chars.next();
        Ok(Some(t))
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        // See https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
        // We don't yet support identifiers beginning with "letters with
        // diacritical marks and non-Latin letters"
        (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '$'
            || ch == '_'
    }
}

/// Read from `chars` until `predicate` returns `false` or EOF is hit.
/// Return the characters read as String, and keep the first non-matching
/// char available as `chars.next()`.
fn peeking_take_while(
    chars: &mut PeekableChars,
    mut predicate: impl FnMut(char) -> bool,
) -> String {
    let mut s = String::new();
    while let Some(ch) = chars.peek() {
        if predicate(ch) {
            chars.next(); // consume
            s.push(ch);
        } else {
            break;
        }
    }
    s
}
