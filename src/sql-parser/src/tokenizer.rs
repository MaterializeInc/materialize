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
    SingleQuotedString(String),
    /// Hexadecimal string literal: i.e.: X'deadbeef'
    HexStringLiteral(String),
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
    /// Json concat operator '||'
    JsonConcat,
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
            Token::SingleQuotedString(ref s) => write!(f, "'{}'", s),
            Token::HexStringLiteral(ref s) => write!(f, "X'{}'", s),
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
            Token::JsonGet => f.write_str("->"),
            Token::JsonGetAsText => f.write_str("->>"),
            Token::JsonGetPath => f.write_str("#>"),
            Token::JsonGetPathAsText => f.write_str("#>>"),
            Token::JsonContainsJson => f.write_str("@>"),
            Token::JsonContainedInJson => f.write_str("<@"),
            Token::JsonContainsField => f.write_str("?"),
            Token::JsonContainsAnyFields => f.write_str("?|"),
            Token::JsonContainsAllFields => f.write_str("?&"),
            Token::JsonConcat => f.write_str("||"),
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

    fn peek(&mut self) -> Option<char> {
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
                // The spec only allows an uppercase 'X' to introduce a hex
                // string, but PostgreSQL, at least, allows a lowercase 'x' too.
                x @ 'x' | x @ 'X' => {
                    chars.next(); // consume, to check the next char
                    match chars.peek() {
                        Some('\'') => {
                            // X'...' - a <binary string literal>
                            let s = self.tokenize_single_quoted_string(chars);
                            Ok(Some(Token::HexStringLiteral(s)))
                        }
                        _ => {
                            // regular identifier starting with an "X"
                            let s = self.tokenize_word(x, chars);
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
                    let s = self.tokenize_single_quoted_string(chars);
                    Ok(Some(Token::SingleQuotedString(s)))
                }
                // delimited (quoted) identifier
                quote_start if self.is_delimited_identifier_start(quote_start) => {
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
                        Some('|') => self.consume_and_return(chars, Token::JsonConcat),
                        _ => Err("Unrecognized token".to_string()),
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
    fn tokenize_single_quoted_string(&self, chars: &mut PeekableChars) -> String {
        //TODO: handle escaped quotes in string
        //TODO: handle newlines in string
        //TODO: handle EOF before terminating quote
        //TODO: handle 'string' <white space> 'string continuation'
        let mut s = String::new();
        chars.next(); // consume the opening quote
        while let Some(ch) = chars.peek() {
            match ch {
                '\'' => {
                    chars.next(); // consume
                    let escaped_quote = chars.peek().map(|c| c == '\'').unwrap_or(false);
                    if escaped_quote {
                        s.push('\'');
                        chars.next();
                    } else {
                        break;
                    }
                }
                _ => {
                    chars.next(); // consume
                    s.push(ch);
                }
            }
        }
        s
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_select_1() {
        let sql = String::from("SELECT 1");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_scalar_function() {
        let sql = String::from("SELECT sqrt(1)");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("sqrt", None),
            Token::LParen,
            Token::Number(String::from("1")),
            Token::RParen,
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_simple_select() {
        let sql = String::from("SELECT * FROM customer WHERE id = 1 LIMIT 5");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("id", None),
            Token::Whitespace(Whitespace::Space),
            Token::Eq,
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("1")),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("LIMIT"),
            Token::Whitespace(Whitespace::Space),
            Token::Number(String::from("5")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_string_predicate() {
        let sql = String::from("SELECT * FROM customer WHERE salary != 'Not Provided'");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();

        let expected = vec![
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("customer", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("WHERE"),
            Token::Whitespace(Whitespace::Space),
            Token::make_word("salary", None),
            Token::Whitespace(Whitespace::Space),
            Token::Neq,
            Token::Whitespace(Whitespace::Space),
            Token::SingleQuotedString(String::from("Not Provided")),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string() {
        let sql = String::from("\nمصطفىh");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_invalid_string_cols() {
        let sql = String::from("\n\nSELECT * FROM table\tمصطفىh");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        println!("tokens: {:#?}", tokens);
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::Newline),
            Token::make_keyword("SELECT"),
            Token::Whitespace(Whitespace::Space),
            Token::Mult,
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("FROM"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("table"),
            Token::Whitespace(Whitespace::Tab),
            Token::Char('م'),
            Token::Char('ص'),
            Token::Char('ط'),
            Token::Char('ف'),
            Token::Char('ى'),
            Token::make_word("h", None),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_is_null() {
        let sql = String::from("a IS NULL");
        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();

        let expected = vec![
            Token::make_word("a", None),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("IS"),
            Token::Whitespace(Whitespace::Space),
            Token::make_keyword("NULL"),
        ];

        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment() {
        let sql = String::from("0--this is a comment\n1");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::SingleLineComment(
                "this is a comment\n".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_comment_at_eof() {
        let sql = String::from("--this is a comment");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        let expected = vec![Token::Whitespace(Whitespace::SingleLineComment(
            "this is a comment".to_string(),
        ))];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment() {
        let sql = String::from("0/*multi-line\n* /comment*/1");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        let expected = vec![
            Token::Number("0".to_string()),
            Token::Whitespace(Whitespace::MultiLineComment(
                "multi-line\n* /comment".to_string(),
            )),
            Token::Number("1".to_string()),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_multiline_comment_with_even_asterisks() {
        let sql = String::from("\n/** Comment **/\n");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        let expected = vec![
            Token::Whitespace(Whitespace::Newline),
            Token::Whitespace(Whitespace::MultiLineComment("* Comment *".to_string())),
            Token::Whitespace(Whitespace::Newline),
        ];
        compare(expected, tokens);
    }

    #[test]
    fn tokenize_mismatched_quotes() {
        let sql = String::from("\"foo");

        let mut tokenizer = Tokenizer::new(&sql);
        assert_eq!(
            tokenizer.tokenize(),
            Err(ParserError {
                sql: sql,
                range: 0..4,
                message: "Expected close delimiter '\"' before EOF.".to_string()
            })
        );
    }

    #[test]
    fn tokenize_newlines() {
        let sql = String::from("line1\nline2\rline3\r\nline4\r");

        let mut tokenizer = Tokenizer::new(&sql);
        let tokens = tokenizer
            .tokenize()
            .unwrap()
            .into_iter()
            .map(|(token, _)| token)
            .collect::<Vec<_>>();
        let expected = vec![
            Token::make_word("line1", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line2", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line3", None),
            Token::Whitespace(Whitespace::Newline),
            Token::make_word("line4", None),
            Token::Whitespace(Whitespace::Newline),
        ];
        compare(expected, tokens);
    }

    fn compare(expected: Vec<Token>, actual: Vec<Token>) {
        //println!("------------------------------");
        //println!("tokens   = {:?}", actual);
        //println!("expected = {:?}", expected);
        //println!("------------------------------");
        assert_eq!(expected, actual);
    }
}
