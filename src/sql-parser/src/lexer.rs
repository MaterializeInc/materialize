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

use std::char;
use std::convert::TryFrom;

use ore::lex::LexBuf;

use crate::keywords::Keyword;
use crate::parser::ParserError;

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

impl Token {
    pub fn name(&self) -> &str {
        match self {
            Token::Keyword(kw) => kw.as_str(),
            Token::Ident(_) => "identifier",
            Token::String(_) => "string literal",
            Token::HexString(_) => "hex string literal",
            Token::Number(_) => "number",
            Token::Parameter(_) => "parameter",
            Token::Op(_) => "operator",
            Token::Star => "star",
            Token::Eq => "equals sign",
            Token::LParen => "left parenthesis",
            Token::RParen => "right parenthesis",
            Token::LBracket => "left square bracket",
            Token::RBracket => "right square bracket",
            Token::Dot => "dot",
            Token::Comma => "comma",
            Token::Colon => "colon",
            Token::DoubleColon => "double colon",
            Token::Semicolon => "semicolon",
        }
    }

    pub fn value(&self) -> String {
        match self {
            Token::Keyword(kw) => kw.as_str().to_string(),
            Token::Ident(val) => val.to_string(),
            Token::String(val) => val.to_string(),
            Token::HexString(val) => val.to_string(),
            Token::Number(val) => val.to_string(),
            Token::Op(val) => val.to_string(),
            Token::Parameter(val) => val.to_string(),
            Token::Star => "*".to_string(),
            Token::Eq => "=".to_string(),
            Token::LParen => "(".to_string(),
            Token::RParen => ")".to_string(),
            Token::LBracket => "[".to_string(),
            Token::RBracket => "]".to_string(),
            Token::Dot => ".".to_string(),
            Token::Comma => ",".to_string(),
            Token::Colon => ":".to_string(),
            Token::DoubleColon => "::".to_string(),
            Token::Semicolon => ";".to_string(),
        }
    }

    /// True iff the `value()` function will return more information in our error messages
    pub fn has_value(&self) -> bool {
        match self {
            Token::Ident(_)
            | Token::String(_)
            | Token::HexString(_)
            | Token::Number(_)
            | Token::Op(_)
            | Token::Parameter(_) => true,
            // In our error messages we use literal keywords, which makes the keyword value a duplicate
            Token::Keyword(_) => false,
            Token::Star
            | Token::Eq
            | Token::LParen
            | Token::RParen
            | Token::LBracket
            | Token::RBracket
            | Token::Dot
            | Token::Comma
            | Token::Colon
            | Token::DoubleColon
            | Token::Semicolon => false,
        }
    }
}

macro_rules! bail {
    ($pos:expr, $($fmt:expr),*) => {
        return Err(ParserError::new($pos, format!($($fmt),*)))
    }
}

/// Lexes a SQL query.
///
/// Returns a list of tokens alongside their corresponding byte offset in the
/// input string. Returns an error if the SQL query is lexically invalid.
///
/// See the module documentation for more information about the lexical
/// structure of SQL.
pub fn lex(query: &str) -> Result<Vec<(Token, usize)>, ParserError> {
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
            'A'..='Z' | 'a'..='z' | '_' | '\u{80}'..=char::MAX => lex_ident(buf),
            '"' => lex_quoted_ident(buf)?,
            '0'..='9' => lex_number(buf),
            '.' if matches!(buf.peek(), Some('0'..='9')) => lex_number(buf),
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
        tokens.push((token, pos))
    }

    #[cfg(debug_assertions)]
    for (_token, pos) in &tokens {
        assert!(query.is_char_boundary(*pos));
    }

    Ok(tokens)
}

fn lex_line_comment(buf: &mut LexBuf) {
    buf.take_while(|ch| ch != '\n');
}

fn lex_multiline_comment(buf: &mut LexBuf) -> Result<(), ParserError> {
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

fn lex_ident(buf: &mut LexBuf) -> Token {
    buf.prev();
    let word = buf.take_while(
        |ch| matches!(ch, 'A'..='Z' | 'a'..='z' | '0'..='9' | '$' | '_' | '\u{80}'..=char::MAX),
    );
    match word.parse() {
        Ok(kw) => Token::Keyword(kw),
        Err(_) => Token::Ident(word.to_lowercase()),
    }
}

fn lex_quoted_ident(buf: &mut LexBuf) -> Result<Token, ParserError> {
    let mut s = String::new();
    let pos = buf.pos() - 1;
    loop {
        match buf.next() {
            Some('"') if buf.consume('"') => s.push('"'),
            Some('"') => break,
            Some(c) => s.push(c),
            None => bail!(pos, "unterminated quoted identifier"),
        }
    }
    Ok(Token::Ident(s))
}

fn lex_string(buf: &mut LexBuf) -> Result<String, ParserError> {
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

fn lex_extended_string(buf: &mut LexBuf) -> Result<Token, ParserError> {
    fn lex_unicode_escape(buf: &mut LexBuf, n: usize) -> Result<char, ParserError> {
        let pos = buf.pos() - 2;
        buf.next_n(n)
            .and_then(|s| u32::from_str_radix(s, 16).ok())
            .and_then(|codepoint| char::try_from(codepoint).ok())
            .ok_or_else(|| ParserError::new(pos, "invalid unicode escape"))
    }

    // We do not support octal (\o) or hexadecimal (\x) escapes, since it is
    // possible to construct invalid UTF-8 with these escapes. We could check
    // for and reject invalid UTF-8, of course, but it is too annoying to be
    // worth doing right now. We still lex the escapes to produce nice error
    // messages.

    fn lex_octal_escape(buf: &mut LexBuf) -> ParserError {
        let pos = buf.pos() - 2;
        buf.take_while(|ch| matches!(ch, '0'..='7'));
        ParserError::new(pos, "octal escapes are not supported")
    }

    fn lex_hexadecimal_escape(buf: &mut LexBuf) -> ParserError {
        let pos = buf.pos() - 2;
        buf.take_while(|ch| matches!(ch, '0'..='9' | 'A'..='F' | 'a'..='f'));
        ParserError::new(pos, "hexadecimal escapes are not supported")
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
    // character. This bizzare rule matches PostgreSQL and the SQL standard.
    let whitespace = buf.take_while(|ch| ch.is_ascii_whitespace());
    whitespace.contains(&['\n', '\r'][..]) && buf.consume('\'')
}

fn lex_dollar_string(buf: &mut LexBuf) -> Result<Token, ParserError> {
    let pos = buf.pos() - 1;
    let tag = format!("${}$", buf.take_while(|ch| ch != '$'));
    if let Some(s) = buf.take_to_delimiter(&tag) {
        Ok(Token::String(s.into()))
    } else {
        Err(ParserError::new(pos, "unterminated dollar-quoted string"))
    }
}

fn lex_parameter(buf: &mut LexBuf) -> Result<Token, ParserError> {
    let pos = buf.pos() - 1;
    let n = buf
        .take_while(|ch| matches!(ch, '0'..='9'))
        .parse()
        .map_err(|_| ParserError::new(pos, "invalid parameter number"))?;
    Ok(Token::Parameter(n))
}

fn lex_number(buf: &mut LexBuf) -> Token {
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
        if buf.consume('-') {
            s.push('-');
        } else {
            buf.consume('+');
        }
        s.push_str(buf.take_while(|ch| matches!(ch, '0'..='9')));
    }

    Token::Number(s)
}

fn lex_op(buf: &mut LexBuf) -> Result<Token, ParserError> {
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
