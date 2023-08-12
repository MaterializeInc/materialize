// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
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

//! SQL lexer.
//!
//! This module lexes SQL according to the rules described in the ["Lexical
//! Structure"] section of the PostgreSQL documentation. The description is
//! intentionally not replicated here. Please refer to that chapter as you
//! read the code in this module.
//!
//! Where the PostgreSQL documentation is unclear, refer to their flex source
//! instead, located in the [backend/parser/scan.l] file in the PostgreSQL
//! Git repository.
//!
//! ["Lexical Structure"]: https://www.postgresql.org/docs/current/sql-syntax-lexical.html
//! [backend/parser/scan.l]: https://github.com/postgres/postgres/blob/90851d1d26f54ccb4d7b1bc49449138113d6ec83/src/backend/parser/scan.l

extern crate alloc;

use std::error::Error;
use std::{char, fmt};

use mz_ore::lex::LexBuf;
use mz_ore::str::StrExt;
use serde::{Deserialize, Serialize};

use crate::keywords::Keyword;

#[cfg(target_arch = "wasm32")]
use lol_alloc::{FreeListAllocator, LockedAllocator};

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[global_allocator]
static ALLOCATOR: LockedAllocator<FreeListAllocator> =
    LockedAllocator::new(FreeListAllocator::new());

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(typescript_custom_section)]
const LEX_TS_DEF: &'static str = r#"export function lex(query: string): PosToken[];"#;

// Maximum allowed identifier length in bytes.
pub const MAX_IDENTIFIER_LENGTH: usize = 255;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LexerError {
    /// The error message.
    pub message: String,
    /// The byte position with which the error is associated.
    pub pos: usize,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for LexerError {}

impl LexerError {
    /// Constructs an error with the provided message at the provided position.
    pub(crate) fn new<S>(pos: usize, message: S) -> LexerError
    where
        S: Into<String>,
    {
        LexerError {
            pos,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),
    Ident(String),
    String(String),
    HexString(String),
    Number(String),
    Parameter(usize),
    Op(String),
    Star,
    Eq,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    Comma,
    Colon,
    DoubleColon,
    Semicolon,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Token::Keyword(kw) => f.write_str(kw.as_str()),
            Token::Ident(id) => write!(f, "identifier {}", id.quoted()),
            Token::String(s) => write!(f, "string literal {}", s.quoted()),
            Token::HexString(s) => write!(f, "hex string literal {}", s.quoted()),
            Token::Number(n) => write!(f, "number \"{}\"", n),
            Token::Parameter(n) => write!(f, "parameter \"${}\"", n),
            Token::Op(op) => write!(f, "operator {}", op.quoted()),
            Token::Star => f.write_str("star"),
            Token::Eq => f.write_str("equals sign"),
            Token::LParen => f.write_str("left parenthesis"),
            Token::RParen => f.write_str("right parenthesis"),
            Token::LBracket => f.write_str("left square bracket"),
            Token::RBracket => f.write_str("right square bracket"),
            Token::Dot => f.write_str("dot"),
            Token::Comma => f.write_str("comma"),
            Token::Colon => f.write_str("colon"),
            Token::DoubleColon => f.write_str("double colon"),
            Token::Semicolon => f.write_str("semicolon"),
        }
    }
}

pub struct PosToken {
    pub kind: Token,
    pub offset: usize,
}

macro_rules! bail {
    ($pos:expr, $($fmt:expr),*) => {
        return Err(LexerError::new($pos, format!($($fmt),*)))
    }
}

/// Lexes a SQL query.
///
/// Returns a list of tokens alongside their corresponding byte offset in the
/// input string. Returns an error if the SQL query is lexically invalid.
///
/// See the module documentation for more information about the lexical
/// structure of SQL.
#[cfg(not(target_arch = "wasm32"))]
pub fn lex(query: &str) -> Result<Vec<PosToken>, LexerError> {
    lex_inner(query)
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = PosToken, getter_with_clone, inspectable)]
#[derive(Debug)]
pub struct JsToken {
    pub kind: String,
    pub offset: usize,
}

#[cfg(target_arch = "wasm32")]
impl From<PosToken> for JsToken {
    fn from(value: PosToken) -> Self {
        JsToken {
            kind: value.kind.to_string(),
            offset: value.offset,
        }
    }
}

/// Lexes a SQL query.
///
/// Returns a list of tokens alongside their corresponding byte offset in the
/// input string. Returns an error if the SQL query is lexically invalid.
///
/// See the module documentation for more information about the lexical
/// structure of SQL.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(skip_typescript)]
pub fn lex(query: &str) -> Result<Vec<JsValue>, JsError> {
    let lexed = lex_inner(query).map_err(|e| JsError::new(&e.message))?;
    Ok(lexed
        .into_iter()
        .map(|token| JsValue::from(JsToken::from(token)))
        .collect())
}

fn lex_inner(query: &str) -> Result<Vec<PosToken>, LexerError> {
    let buf = &mut LexBuf::new(query);
    let mut tokens = vec![];
    while let Some(ch) = buf.next() {
        let pos = buf.pos() - ch.len_utf8();
        let token = match ch {
            _ if ch.is_ascii_whitespace() => continue,
            '-' if buf.consume('-') => {
                lex_line_comment(buf);
                continue;
            }
            '/' if buf.consume('*') => {
                lex_multiline_comment(buf)?;
                continue;
            }
            '\'' => Token::String(lex_string(buf)?),
            'x' | 'X' if buf.consume('\'') => Token::HexString(lex_string(buf)?),
            'e' | 'E' if buf.consume('\'') => lex_extended_string(buf)?,
            'A'..='Z' | 'a'..='z' | '_' | '\u{80}'..=char::MAX => lex_ident(buf)?,
            '"' => lex_quoted_ident(buf)?,
            '0'..='9' => lex_number(buf)?,
            '.' if matches!(buf.peek(), Some('0'..='9')) => lex_number(buf)?,
            '$' if matches!(buf.peek(), Some('0'..='9')) => lex_parameter(buf)?,
            '$' => lex_dollar_string(buf)?,
            '(' => Token::LParen,
            ')' => Token::RParen,
            ',' => Token::Comma,
            '.' => Token::Dot,
            ':' if buf.consume(':') => Token::DoubleColon,
            ':' => Token::Colon,
            ';' => Token::Semicolon,
            '[' => Token::LBracket,
            ']' => Token::RBracket,
            #[rustfmt::skip]
            '+'|'-'|'*'|'/'|'<'|'>'|'='|'~'|'!'|'@'|'#'|'%'|'^'|'&'|'|'|'`'|'?' => lex_op(buf)?,
            _ => bail!(pos, "unexpected character in input: {}", ch),
        };
        tokens.push(PosToken {
            kind: token,
            offset: pos,
        })
    }

    #[cfg(debug_assertions)]
    for token in &tokens {
        assert!(query.is_char_boundary(token.offset));
    }

    Ok(tokens)
}

fn lex_line_comment(buf: &mut LexBuf) {
    buf.take_while(|ch| ch != '\n');
}

fn lex_multiline_comment(buf: &mut LexBuf) -> Result<(), LexerError> {
    let pos = buf.pos() - 2;
    let mut nesting = 0;
    while let Some(ch) = buf.next() {
        match ch {
            '*' if buf.consume('/') => {
                if nesting == 0 {
                    return Ok(());
                } else {
                    nesting -= 1;
                }
            }
            '/' if buf.consume('*') => nesting += 1,
            _ => (),
        }
    }
    bail!(pos, "unterminated multiline comment")
}

fn lex_ident(buf: &mut LexBuf) -> Result<Token, LexerError> {
    buf.prev();
    let pos: usize = buf.pos();
    let word = buf.take_while(
        |ch| matches!(ch, 'A'..='Z' | 'a'..='z' | '0'..='9' | '$' | '_' | '\u{80}'..=char::MAX),
    );
    match word.parse() {
        Ok(kw) => Ok(Token::Keyword(kw)),
        Err(_) => {
            if word.len() > MAX_IDENTIFIER_LENGTH {
                bail!(
                    pos,
                    "identifier length exceeds {MAX_IDENTIFIER_LENGTH} bytes"
                )
            }
            Ok(Token::Ident(word.to_lowercase()))
        }
    }
}

fn lex_quoted_ident(buf: &mut LexBuf) -> Result<Token, LexerError> {
    let mut s = String::new();
    let pos = buf.pos() - 1;
    loop {
        match buf.next() {
            Some('"') if buf.consume('"') => s.push('"'),
            Some('"') => break,
            Some('\0') => bail!(pos, "null character in quoted identifier"),
            Some(c) => s.push(c),
            None => bail!(pos, "unterminated quoted identifier"),
        }
    }
    if s.len() > MAX_IDENTIFIER_LENGTH {
        bail!(
            pos,
            "identifier length exceeds {MAX_IDENTIFIER_LENGTH} bytes"
        )
    }
    Ok(Token::Ident(s))
}

fn lex_string(buf: &mut LexBuf) -> Result<String, LexerError> {
    let mut s = String::new();
    loop {
        let pos = buf.pos() - 1;
        loop {
            match buf.next() {
                Some('\'') if buf.consume('\'') => s.push('\''),
                Some('\'') => break,
                Some(c) => s.push(c),
                None => bail!(pos, "unterminated quoted string"),
            }
        }
        if !lex_to_adjacent_string(buf) {
            return Ok(s);
        }
    }
}

fn lex_extended_string(buf: &mut LexBuf) -> Result<Token, LexerError> {
    fn lex_unicode_escape(buf: &mut LexBuf, n: usize) -> Result<char, LexerError> {
        let pos = buf.pos() - 2;
        buf.next_n(n)
            .and_then(|s| u32::from_str_radix(s, 16).ok())
            .and_then(|codepoint| char::try_from(codepoint).ok())
            .ok_or_else(|| LexerError::new(pos, "invalid unicode escape"))
    }

    // We do not support octal (\o) or hexadecimal (\x) escapes, since it is
    // possible to construct invalid UTF-8 with these escapes. We could check
    // for and reject invalid UTF-8, of course, but it is too annoying to be
    // worth doing right now. We still lex the escapes to produce nice error
    // messages.

    fn lex_octal_escape(buf: &mut LexBuf) -> LexerError {
        let pos = buf.pos() - 2;
        buf.take_while(|ch| matches!(ch, '0'..='7'));
        LexerError::new(pos, "octal escapes are not supported")
    }

    fn lex_hexadecimal_escape(buf: &mut LexBuf) -> LexerError {
        let pos = buf.pos() - 2;
        buf.take_while(|ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'));
        LexerError::new(pos, "hexadecimal escapes are not supported")
    }

    let mut s = String::new();
    loop {
        let pos = buf.pos() - 1;
        loop {
            match buf.next() {
                Some('\'') if buf.consume('\'') => s.push('\''),
                Some('\'') => break,
                Some('\\') => match buf.next() {
                    Some('b') => s.push('\x08'),
                    Some('f') => s.push('\x0c'),
                    Some('n') => s.push('\n'),
                    Some('r') => s.push('\r'),
                    Some('t') => s.push('\t'),
                    Some('u') => s.push(lex_unicode_escape(buf, 4)?),
                    Some('U') => s.push(lex_unicode_escape(buf, 8)?),
                    Some('0'..='7') => return Err(lex_octal_escape(buf)),
                    Some('x') => return Err(lex_hexadecimal_escape(buf)),
                    Some(c) => s.push(c),
                    None => bail!(pos, "unterminated quoted string"),
                },
                Some(c) => s.push(c),
                None => bail!(pos, "unterminated quoted string"),
            }
        }
        if !lex_to_adjacent_string(buf) {
            return Ok(Token::String(s));
        }
    }
}

fn lex_to_adjacent_string(buf: &mut LexBuf) -> bool {
    // Adjacent string literals that are separated by whitespace are
    // concatenated if and only if that whitespace contains at least one newline
    // character. This bizarre rule matches PostgreSQL and the SQL standard.
    let whitespace = buf.take_while(|ch| ch.is_ascii_whitespace());
    whitespace.contains(&['\n', '\r'][..]) && buf.consume('\'')
}

fn lex_dollar_string(buf: &mut LexBuf) -> Result<Token, LexerError> {
    let pos = buf.pos() - 1;
    let tag = format!("${}$", buf.take_while(|ch| ch != '$'));
    let _ = buf.next();
    if let Some(s) = buf.take_to_delimiter(&tag) {
        Ok(Token::String(s.into()))
    } else {
        Err(LexerError::new(pos, "unterminated dollar-quoted string"))
    }
}

fn lex_parameter(buf: &mut LexBuf) -> Result<Token, LexerError> {
    let pos = buf.pos() - 1;
    let n = buf
        .take_while(|ch| matches!(ch, '0'..='9'))
        .parse()
        .map_err(|_| LexerError::new(pos, "invalid parameter number"))?;
    Ok(Token::Parameter(n))
}

fn lex_number(buf: &mut LexBuf) -> Result<Token, LexerError> {
    buf.prev();
    let mut s = buf.take_while(|ch| matches!(ch, '0'..='9')).to_owned();

    // Optional decimal component.
    if buf.consume('.') {
        s.push('.');
        s.push_str(buf.take_while(|ch| matches!(ch, '0'..='9')));
    }

    // Optional exponent.
    if buf.consume('e') || buf.consume('E') {
        s.push('E');
        let require_exp = if buf.consume('-') {
            s.push('-');
            true
        } else {
            buf.consume('+')
        };
        let exp = buf.take_while(|ch| matches!(ch, '0'..='9'));
        if require_exp && exp.is_empty() {
            return Err(LexerError::new(buf.pos() - 1, "missing required exponent"));
        } else if exp.is_empty() {
            // Put back consumed E.
            buf.prev();
            s.pop();
        } else {
            s.push_str(exp);
        }
    }

    Ok(Token::Number(s))
}

fn lex_op(buf: &mut LexBuf) -> Result<Token, LexerError> {
    buf.prev();
    let mut s = String::new();

    // In PostgreSQL, operators might be composed of any of the characters in
    // the set below...
    while let Some(ch) = buf.next() {
        match ch {
            // ...except the sequences `--` and `/*` start comments, even within
            // what would otherwise be an operator...
            '-' if buf.consume('-') => lex_line_comment(buf),
            '/' if buf.consume('*') => lex_multiline_comment(buf)?,
            #[rustfmt::skip]
            '+'|'-'|'*'|'/'|'<'|'>'|'='|'~'|'!'|'@'|'#'|'%'|'^'|'&'|'|'|'`'|'?' => s.push(ch),
            _ => {
                buf.prev();
                break;
            }
        }
    }

    // ...and a multi-character operator that ends with `-` or `+` must also
    // contain at least one nonstandard operator character. This is so that e.g.
    // `1+-2` is lexed as `1 + (-2)` as required by the SQL standard, but `1@+2`
    // is lexed as `1 @+ 2`, as `@+` is meant to be a user-definable operator.
    if s.len() > 1
        && s.ends_with(&['-', '+'][..])
        && !s.contains(&['~', '!', '@', '#', '%', '^', '&', '|', '`', '?'][..])
    {
        while s.len() > 1 && s.ends_with(&['-', '+'][..]) {
            buf.prev();
            s.pop();
        }
    }

    match s.as_str() {
        // `*` and `=` are not just expression operators in SQL, so give them
        // dedicated tokens to simplify the parser.
        "*" => Ok(Token::Star),
        "=" => Ok(Token::Eq),
        // Normalize the two forms of the not-equals operator.
        "!=" => Ok(Token::Op("<>".into())),
        // Emit all other operators as is.
        _ => Ok(Token::Op(s)),
    }
}
