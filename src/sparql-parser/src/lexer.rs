// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL 1.1 lexer.
//!
//! Tokenizes SPARQL query strings into a sequence of [`Token`]s. Follows the
//! lexical rules from [W3C SPARQL 1.1 Query Language, Section 19](
//! https://www.w3.org/TR/sparql11-query/#sparqlGrammar).

use std::fmt;

/// A SPARQL keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Keyword {
    Select,
    Construct,
    Describe,
    Ask,
    Where,
    From,
    Named,
    Prefix,
    Base,
    Optional,
    Filter,
    Union,
    Minus,
    Bind,
    Values,
    Graph,
    Service,
    Silent,
    GroupBy,
    OrderBy,
    Having,
    Limit,
    Offset,
    Distinct,
    Reduced,
    As,
    Asc,
    Desc,
    Exists,
    NotExists,
    // `a` is a keyword shorthand for rdf:type
    A,
    // Boolean literals
    True,
    False,
    // Logical operators
    Not,
    And,
    Or,
    In,
    // Type testing functions
    IsIri,
    IsUri,
    IsBlank,
    IsLiteral,
    IsNumeric,
    // Accessor functions
    Str,
    Lang,
    Langmatches,
    Datatype,
    Iri,
    Uri,
    Bnode,
    // String functions
    Strlen,
    Substr,
    Ucase,
    Lcase,
    Strstarts,
    Strends,
    Contains,
    Concat,
    Replace,
    Regex,
    // Numeric functions
    Abs,
    Round,
    Ceil,
    Floor,
    Rand,
    // Date/time functions
    Now,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Timezone,
    Tz,
    // Hash functions
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
    // Aggregate functions
    Count,
    Sum,
    Avg,
    Min,
    Max,
    GroupConcat,
    Sample,
    Separator,
    // Other built-in functions
    Bound,
    If,
    Coalesce,
    SameTerm,
    StrDt,
    StrLang,
    EncodeForUri,
    Uuid,
    Struuid,
    Undef,
}

impl Keyword {
    /// Try to match a case-insensitive identifier to a keyword.
    pub fn from_str_case_insensitive(s: &str) -> Option<Keyword> {
        // SPARQL keywords are case-insensitive.
        match s.to_ascii_uppercase().as_str() {
            "SELECT" => Some(Keyword::Select),
            "CONSTRUCT" => Some(Keyword::Construct),
            "DESCRIBE" => Some(Keyword::Describe),
            "ASK" => Some(Keyword::Ask),
            "WHERE" => Some(Keyword::Where),
            "FROM" => Some(Keyword::From),
            "NAMED" => Some(Keyword::Named),
            "PREFIX" => Some(Keyword::Prefix),
            "BASE" => Some(Keyword::Base),
            "OPTIONAL" => Some(Keyword::Optional),
            "FILTER" => Some(Keyword::Filter),
            "UNION" => Some(Keyword::Union),
            "MINUS" => Some(Keyword::Minus),
            "BIND" => Some(Keyword::Bind),
            "VALUES" => Some(Keyword::Values),
            "GRAPH" => Some(Keyword::Graph),
            "SERVICE" => Some(Keyword::Service),
            "SILENT" => Some(Keyword::Silent),
            "GROUP" => Some(Keyword::GroupBy), // GROUP is always followed by BY
            "ORDER" => Some(Keyword::OrderBy), // ORDER is always followed by BY
            "HAVING" => Some(Keyword::Having),
            "LIMIT" => Some(Keyword::Limit),
            "OFFSET" => Some(Keyword::Offset),
            "DISTINCT" => Some(Keyword::Distinct),
            "REDUCED" => Some(Keyword::Reduced),
            "AS" => Some(Keyword::As),
            "ASC" => Some(Keyword::Asc),
            "DESC" => Some(Keyword::Desc),
            "EXISTS" => Some(Keyword::Exists),
            "NOT" => Some(Keyword::Not),
            "A" => Some(Keyword::A),
            "TRUE" => Some(Keyword::True),
            "FALSE" => Some(Keyword::False),
            "AND" => Some(Keyword::And),
            "OR" => Some(Keyword::Or),
            "IN" => Some(Keyword::In),
            "ISIRI" => Some(Keyword::IsIri),
            "ISURI" => Some(Keyword::IsUri),
            "ISBLANK" => Some(Keyword::IsBlank),
            "ISLITERAL" => Some(Keyword::IsLiteral),
            "ISNUMERIC" => Some(Keyword::IsNumeric),
            "STR" => Some(Keyword::Str),
            "LANG" => Some(Keyword::Lang),
            "LANGMATCHES" => Some(Keyword::Langmatches),
            "DATATYPE" => Some(Keyword::Datatype),
            "IRI" => Some(Keyword::Iri),
            "URI" => Some(Keyword::Uri),
            "BNODE" => Some(Keyword::Bnode),
            "STRLEN" => Some(Keyword::Strlen),
            "SUBSTR" => Some(Keyword::Substr),
            "UCASE" => Some(Keyword::Ucase),
            "LCASE" => Some(Keyword::Lcase),
            "STRSTARTS" => Some(Keyword::Strstarts),
            "STRENDS" => Some(Keyword::Strends),
            "CONTAINS" => Some(Keyword::Contains),
            "CONCAT" => Some(Keyword::Concat),
            "REPLACE" => Some(Keyword::Replace),
            "REGEX" => Some(Keyword::Regex),
            "ABS" => Some(Keyword::Abs),
            "ROUND" => Some(Keyword::Round),
            "CEIL" => Some(Keyword::Ceil),
            "FLOOR" => Some(Keyword::Floor),
            "RAND" => Some(Keyword::Rand),
            "NOW" => Some(Keyword::Now),
            "YEAR" => Some(Keyword::Year),
            "MONTH" => Some(Keyword::Month),
            "DAY" => Some(Keyword::Day),
            "HOURS" => Some(Keyword::Hours),
            "MINUTES" => Some(Keyword::Minutes),
            "SECONDS" => Some(Keyword::Seconds),
            "TIMEZONE" => Some(Keyword::Timezone),
            "TZ" => Some(Keyword::Tz),
            "MD5" => Some(Keyword::Md5),
            "SHA1" => Some(Keyword::Sha1),
            "SHA256" => Some(Keyword::Sha256),
            "SHA384" => Some(Keyword::Sha384),
            "SHA512" => Some(Keyword::Sha512),
            "COUNT" => Some(Keyword::Count),
            "SUM" => Some(Keyword::Sum),
            "AVG" => Some(Keyword::Avg),
            "MIN" => Some(Keyword::Min),
            "MAX" => Some(Keyword::Max),
            "GROUP_CONCAT" => Some(Keyword::GroupConcat),
            "SAMPLE" => Some(Keyword::Sample),
            "SEPARATOR" => Some(Keyword::Separator),
            "BOUND" => Some(Keyword::Bound),
            "IF" => Some(Keyword::If),
            "COALESCE" => Some(Keyword::Coalesce),
            "SAMETERM" => Some(Keyword::SameTerm),
            "STRDT" => Some(Keyword::StrDt),
            "STRLANG" => Some(Keyword::StrLang),
            "ENCODE_FOR_URI" => Some(Keyword::EncodeForUri),
            "UUID" => Some(Keyword::Uuid),
            "STRUUID" => Some(Keyword::Struuid),
            "UNDEF" => Some(Keyword::Undef),
            _ => None,
        }
    }
}

impl fmt::Display for Keyword {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Keyword::Select => "SELECT",
            Keyword::Construct => "CONSTRUCT",
            Keyword::Describe => "DESCRIBE",
            Keyword::Ask => "ASK",
            Keyword::Where => "WHERE",
            Keyword::From => "FROM",
            Keyword::Named => "NAMED",
            Keyword::Prefix => "PREFIX",
            Keyword::Base => "BASE",
            Keyword::Optional => "OPTIONAL",
            Keyword::Filter => "FILTER",
            Keyword::Union => "UNION",
            Keyword::Minus => "MINUS",
            Keyword::Bind => "BIND",
            Keyword::Values => "VALUES",
            Keyword::Graph => "GRAPH",
            Keyword::Service => "SERVICE",
            Keyword::Silent => "SILENT",
            Keyword::GroupBy => "GROUP BY",
            Keyword::OrderBy => "ORDER BY",
            Keyword::Having => "HAVING",
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
            Keyword::Distinct => "DISTINCT",
            Keyword::Reduced => "REDUCED",
            Keyword::As => "AS",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Exists => "EXISTS",
            Keyword::NotExists => "NOT EXISTS",
            Keyword::A => "a",
            Keyword::True => "true",
            Keyword::False => "false",
            Keyword::Not => "NOT",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::In => "IN",
            Keyword::IsIri => "isIRI",
            Keyword::IsUri => "isURI",
            Keyword::IsBlank => "isBlank",
            Keyword::IsLiteral => "isLiteral",
            Keyword::IsNumeric => "isNumeric",
            Keyword::Str => "STR",
            Keyword::Lang => "LANG",
            Keyword::Langmatches => "LANGMATCHES",
            Keyword::Datatype => "DATATYPE",
            Keyword::Iri => "IRI",
            Keyword::Uri => "URI",
            Keyword::Bnode => "BNODE",
            Keyword::Strlen => "STRLEN",
            Keyword::Substr => "SUBSTR",
            Keyword::Ucase => "UCASE",
            Keyword::Lcase => "LCASE",
            Keyword::Strstarts => "STRSTARTS",
            Keyword::Strends => "STRENDS",
            Keyword::Contains => "CONTAINS",
            Keyword::Concat => "CONCAT",
            Keyword::Replace => "REPLACE",
            Keyword::Regex => "REGEX",
            Keyword::Abs => "ABS",
            Keyword::Round => "ROUND",
            Keyword::Ceil => "CEIL",
            Keyword::Floor => "FLOOR",
            Keyword::Rand => "RAND",
            Keyword::Now => "NOW",
            Keyword::Year => "YEAR",
            Keyword::Month => "MONTH",
            Keyword::Day => "DAY",
            Keyword::Hours => "HOURS",
            Keyword::Minutes => "MINUTES",
            Keyword::Seconds => "SECONDS",
            Keyword::Timezone => "TIMEZONE",
            Keyword::Tz => "TZ",
            Keyword::Md5 => "MD5",
            Keyword::Sha1 => "SHA1",
            Keyword::Sha256 => "SHA256",
            Keyword::Sha384 => "SHA384",
            Keyword::Sha512 => "SHA512",
            Keyword::Count => "COUNT",
            Keyword::Sum => "SUM",
            Keyword::Avg => "AVG",
            Keyword::Min => "MIN",
            Keyword::Max => "MAX",
            Keyword::GroupConcat => "GROUP_CONCAT",
            Keyword::Sample => "SAMPLE",
            Keyword::Separator => "SEPARATOR",
            Keyword::Bound => "BOUND",
            Keyword::If => "IF",
            Keyword::Coalesce => "COALESCE",
            Keyword::SameTerm => "sameTerm",
            Keyword::StrDt => "STRDT",
            Keyword::StrLang => "STRLANG",
            Keyword::EncodeForUri => "ENCODE_FOR_URI",
            Keyword::Uuid => "UUID",
            Keyword::Struuid => "STRUUID",
            Keyword::Undef => "UNDEF",
        };
        write!(f, "{}", s)
    }
}

/// A SPARQL token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A SPARQL keyword.
    Keyword(Keyword),
    /// A variable: `?name` or `$name`.
    Variable(String),
    /// A full IRI: `<http://example.org/foo>`.
    Iri(String),
    /// A prefixed name: `ex:foo`. The prefix and local parts are stored separately.
    PrefixedName { prefix: String, local: String },
    /// A blank node label: `_:b0`.
    BlankNodeLabel(String),
    /// A string literal (double-quoted or single-quoted).
    StringLiteral(String),
    /// A long string literal (triple-quoted).
    LongStringLiteral(String),
    /// An integer literal.
    Integer(String),
    /// A decimal literal (has a dot but no exponent).
    Decimal(String),
    /// A double literal (has an exponent).
    Double(String),
    /// Language tag following a string literal (e.g., `@en`).
    LangTag(String),
    /// `^^` — datatype separator following a string literal.
    DoubleCaret,
    /// `.` — triple terminator.
    Dot,
    /// `,` — object list separator.
    Comma,
    /// `;` — predicate-object list separator.
    Semicolon,
    /// `(` — left parenthesis.
    LParen,
    /// `)` — right parenthesis.
    RParen,
    /// `{` — left brace.
    LBrace,
    /// `}` — right brace.
    RBrace,
    /// `[` — left bracket (for blank node syntax).
    LBracket,
    /// `]` — right bracket.
    RBracket,
    /// `*` — wildcard / multiplication / path zero-or-more.
    Star,
    /// `+` — addition / path one-or-more.
    Plus,
    /// `-` — subtraction.
    Minus,
    /// `/` — division / path sequence.
    Slash,
    /// `=` — equality.
    Eq,
    /// `!=` — inequality.
    NotEq,
    /// `<` — less than (also starts IRIs but handled in lexer).
    Lt,
    /// `>` — greater than.
    Gt,
    /// `<=` — less than or equal.
    LtEq,
    /// `>=` — greater than or equal.
    GtEq,
    /// `&&` — logical AND.
    AmpAmp,
    /// `||` — logical OR.
    PipePipe,
    /// `!` — logical NOT / negated property set.
    Bang,
    /// `^` — inverse path.
    Caret,
    /// `|` — alternative path.
    Pipe,
    /// `?` — path zero-or-one (only when following a path expression, not starting a variable).
    Question,
}

/// A token with its byte offset in the source string.
#[derive(Debug, Clone, PartialEq)]
pub struct PosToken {
    pub kind: Token,
    pub offset: usize,
}

/// An error encountered during lexing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexerError {
    pub message: String,
    pub pos: usize,
}

impl fmt::Display for LexerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "lexer error at byte {}: {}", self.pos, self.message)
    }
}

impl std::error::Error for LexerError {}

/// Tokenize a SPARQL query string into a sequence of positioned tokens.
pub fn lex(input: &str) -> Result<Vec<PosToken>, LexerError> {
    let mut tokens = Vec::new();
    let bytes = input.as_bytes();
    let mut pos = 0;

    while pos < bytes.len() {
        // Skip whitespace.
        if bytes[pos].is_ascii_whitespace() {
            pos += 1;
            continue;
        }

        // Skip comments (# to end of line).
        if bytes[pos] == b'#' {
            while pos < bytes.len() && bytes[pos] != b'\n' {
                pos += 1;
            }
            continue;
        }

        let start = pos;
        let token = lex_token(input, &mut pos)?;
        tokens.push(PosToken {
            kind: token,
            offset: start,
        });
    }

    Ok(tokens)
}

fn lex_token(input: &str, pos: &mut usize) -> Result<Token, LexerError> {
    let bytes = input.as_bytes();
    let start = *pos;
    let ch = bytes[start];

    match ch {
        // IRI: <...>
        b'<' => {
            // Check for <= operator.
            if start + 1 < bytes.len() && bytes[start + 1] == b'=' {
                *pos = start + 2;
                return Ok(Token::LtEq);
            }
            // Check if this looks like an IRI (contains a colon or slash typically)
            // vs a less-than operator. In SPARQL, < starts an IRI if followed by
            // a valid IRI character. A bare < before a space/operator is less-than.
            // Heuristic: if the next char is whitespace or end, it's less-than.
            if start + 1 >= bytes.len() || bytes[start + 1].is_ascii_whitespace() {
                *pos = start + 1;
                return Ok(Token::Lt);
            }
            // Try to lex as IRI.
            *pos = start + 1;
            let iri_start = *pos;
            while *pos < bytes.len() && bytes[*pos] != b'>' {
                if bytes[*pos] == b'\n' || bytes[*pos] == b'\r' {
                    return Err(LexerError {
                        message: "unterminated IRI".to_string(),
                        pos: start,
                    });
                }
                *pos += 1;
            }
            if *pos >= bytes.len() {
                return Err(LexerError {
                    message: "unterminated IRI".to_string(),
                    pos: start,
                });
            }
            let iri = input[iri_start..*pos].to_string();
            *pos += 1; // skip >
            Ok(Token::Iri(iri))
        }

        // Variable: ?name or $name
        b'?' | b'$' => {
            // `?` after certain contexts could be the zero-or-one path operator,
            // but we distinguish this in the parser, not the lexer.
            // If the next char is a valid identifier start, it's a variable.
            if start + 1 < bytes.len() && is_varname_start(bytes[start + 1]) {
                *pos = start + 1;
                while *pos < bytes.len() && is_varname_char(bytes[*pos]) {
                    *pos += 1;
                }
                let name = input[start + 1..*pos].to_string();
                Ok(Token::Variable(name))
            } else if ch == b'?' {
                *pos = start + 1;
                Ok(Token::Question)
            } else {
                return Err(LexerError {
                    message: format!("unexpected character '{}'", ch as char),
                    pos: start,
                });
            }
        }

        // String literal (double-quoted)
        b'"' => lex_string_literal(input, pos),

        // String literal (single-quoted)
        b'\'' => lex_string_literal(input, pos),

        // Blank node label: _:name
        b'_' if start + 1 < bytes.len() && bytes[start + 1] == b':' => {
            *pos = start + 2;
            if *pos >= bytes.len() || !is_varname_start(bytes[*pos]) {
                return Err(LexerError {
                    message: "expected blank node label after '_:'".to_string(),
                    pos: start,
                });
            }
            while *pos < bytes.len() && is_varname_char(bytes[*pos]) {
                *pos += 1;
            }
            let label = input[start + 2..*pos].to_string();
            Ok(Token::BlankNodeLabel(label))
        }

        // Numeric literals
        b'0'..=b'9' => lex_numeric(input, pos),

        // Dot — could be decimal number or triple terminator
        b'.' => {
            if start + 1 < bytes.len() && bytes[start + 1].is_ascii_digit() {
                lex_numeric(input, pos)
            } else {
                *pos = start + 1;
                Ok(Token::Dot)
            }
        }

        // Language tag: @en
        b'@' => {
            *pos = start + 1;
            if *pos >= bytes.len() || !bytes[*pos].is_ascii_alphabetic() {
                return Err(LexerError {
                    message: "expected language tag after '@'".to_string(),
                    pos: start,
                });
            }
            while *pos < bytes.len() && (bytes[*pos].is_ascii_alphanumeric() || bytes[*pos] == b'-')
            {
                *pos += 1;
            }
            let tag = input[start + 1..*pos].to_string();
            Ok(Token::LangTag(tag))
        }

        // Colon: empty prefix `:localPart` or bare `:` (empty prefix, empty local).
        b':' => {
            *pos = start + 1;
            let local_start = *pos;
            while *pos < bytes.len() && is_pn_local_char(bytes[*pos]) {
                *pos += 1;
            }
            let local = input[local_start..*pos].to_string();
            Ok(Token::PrefixedName {
                prefix: String::new(),
                local,
            })
        }

        // Operators and punctuation
        b'{' => {
            *pos = start + 1;
            Ok(Token::LBrace)
        }
        b'}' => {
            *pos = start + 1;
            Ok(Token::RBrace)
        }
        b'(' => {
            *pos = start + 1;
            Ok(Token::LParen)
        }
        b')' => {
            *pos = start + 1;
            Ok(Token::RParen)
        }
        b'[' => {
            *pos = start + 1;
            Ok(Token::LBracket)
        }
        b']' => {
            *pos = start + 1;
            Ok(Token::RBracket)
        }
        b',' => {
            *pos = start + 1;
            Ok(Token::Comma)
        }
        b';' => {
            *pos = start + 1;
            Ok(Token::Semicolon)
        }
        b'*' => {
            *pos = start + 1;
            Ok(Token::Star)
        }
        b'+' => {
            *pos = start + 1;
            Ok(Token::Plus)
        }
        b'-' => {
            *pos = start + 1;
            Ok(Token::Minus)
        }
        b'/' => {
            *pos = start + 1;
            Ok(Token::Slash)
        }
        b'=' => {
            *pos = start + 1;
            Ok(Token::Eq)
        }
        b'>' => {
            if start + 1 < bytes.len() && bytes[start + 1] == b'=' {
                *pos = start + 2;
                Ok(Token::GtEq)
            } else {
                *pos = start + 1;
                Ok(Token::Gt)
            }
        }
        b'!' => {
            if start + 1 < bytes.len() && bytes[start + 1] == b'=' {
                *pos = start + 2;
                Ok(Token::NotEq)
            } else {
                *pos = start + 1;
                Ok(Token::Bang)
            }
        }
        b'&' if start + 1 < bytes.len() && bytes[start + 1] == b'&' => {
            *pos = start + 2;
            Ok(Token::AmpAmp)
        }
        b'|' => {
            if start + 1 < bytes.len() && bytes[start + 1] == b'|' {
                *pos = start + 2;
                Ok(Token::PipePipe)
            } else {
                *pos = start + 1;
                Ok(Token::Pipe)
            }
        }
        b'^' => {
            if start + 1 < bytes.len() && bytes[start + 1] == b'^' {
                *pos = start + 2;
                Ok(Token::DoubleCaret)
            } else {
                *pos = start + 1;
                Ok(Token::Caret)
            }
        }

        // Identifiers (keywords or prefixed names)
        _ if is_pn_chars_base(ch) => lex_identifier_or_prefixed(input, pos),

        _ => Err(LexerError {
            message: format!("unexpected character '{}'", ch as char),
            pos: start,
        }),
    }
}

/// Lex a string literal (single or double quoted, short or long form).
fn lex_string_literal(input: &str, pos: &mut usize) -> Result<Token, LexerError> {
    let bytes = input.as_bytes();
    let start = *pos;
    let quote = bytes[start];

    // Check for long string (triple-quoted).
    if start + 2 < bytes.len() && bytes[start + 1] == quote && bytes[start + 2] == quote {
        *pos = start + 3;
        let content_start = *pos;
        loop {
            if *pos + 2 >= bytes.len() {
                return Err(LexerError {
                    message: "unterminated long string literal".to_string(),
                    pos: start,
                });
            }
            if bytes[*pos] == quote && bytes[*pos + 1] == quote && bytes[*pos + 2] == quote {
                let content = input[content_start..*pos].to_string();
                *pos += 3;
                return Ok(Token::LongStringLiteral(content));
            }
            if bytes[*pos] == b'\\' {
                *pos += 2; // skip escape sequence
            } else {
                *pos += 1;
            }
        }
    }

    // Short string literal.
    *pos = start + 1;
    let mut content = String::new();
    loop {
        if *pos >= bytes.len() {
            return Err(LexerError {
                message: "unterminated string literal".to_string(),
                pos: start,
            });
        }
        if bytes[*pos] == quote {
            *pos += 1;
            return Ok(Token::StringLiteral(content));
        }
        if bytes[*pos] == b'\\' {
            *pos += 1;
            if *pos >= bytes.len() {
                return Err(LexerError {
                    message: "unterminated escape sequence in string".to_string(),
                    pos: start,
                });
            }
            match bytes[*pos] {
                b't' => content.push('\t'),
                b'n' => content.push('\n'),
                b'r' => content.push('\r'),
                b'\\' => content.push('\\'),
                b'\'' => content.push('\''),
                b'"' => content.push('"'),
                _ => {
                    // For unrecognized escapes, include both characters.
                    content.push('\\');
                    content.push(bytes[*pos] as char);
                }
            }
            *pos += 1;
        } else if bytes[*pos] == b'\n' || bytes[*pos] == b'\r' {
            return Err(LexerError {
                message: "unterminated string literal (newline in short string)".to_string(),
                pos: start,
            });
        } else {
            // Handle multi-byte UTF-8 characters properly.
            let ch = next_char(input, pos);
            content.push(ch);
        }
    }
}

/// Lex a numeric literal: integer, decimal, or double.
fn lex_numeric(input: &str, pos: &mut usize) -> Result<Token, LexerError> {
    let bytes = input.as_bytes();
    let start = *pos;
    let mut has_dot = false;
    let mut has_exp = false;

    // Integer part (may start with `.` for decimals like `.5`).
    if bytes[*pos] == b'.' {
        has_dot = true;
        *pos += 1;
    }

    while *pos < bytes.len() && bytes[*pos].is_ascii_digit() {
        *pos += 1;
    }

    // Decimal part.
    if !has_dot && *pos < bytes.len() && bytes[*pos] == b'.' {
        // Only treat as decimal if followed by a digit (avoid consuming `.` separator).
        if *pos + 1 < bytes.len() && bytes[*pos + 1].is_ascii_digit() {
            has_dot = true;
            *pos += 1;
            while *pos < bytes.len() && bytes[*pos].is_ascii_digit() {
                *pos += 1;
            }
        }
    }

    // Exponent part.
    if *pos < bytes.len() && (bytes[*pos] == b'e' || bytes[*pos] == b'E') {
        has_exp = true;
        *pos += 1;
        if *pos < bytes.len() && (bytes[*pos] == b'+' || bytes[*pos] == b'-') {
            *pos += 1;
        }
        if *pos >= bytes.len() || !bytes[*pos].is_ascii_digit() {
            return Err(LexerError {
                message: "expected digits in exponent".to_string(),
                pos: start,
            });
        }
        while *pos < bytes.len() && bytes[*pos].is_ascii_digit() {
            *pos += 1;
        }
    }

    let text = input[start..*pos].to_string();
    if has_exp {
        Ok(Token::Double(text))
    } else if has_dot {
        Ok(Token::Decimal(text))
    } else {
        Ok(Token::Integer(text))
    }
}

/// Lex an identifier, keyword, or prefixed name.
fn lex_identifier_or_prefixed(input: &str, pos: &mut usize) -> Result<Token, LexerError> {
    let bytes = input.as_bytes();
    let start = *pos;

    // Consume the identifier.
    while *pos < bytes.len() && is_pn_chars(bytes[*pos]) {
        *pos += 1;
    }

    // Check for prefixed name: `prefix:local`.
    if *pos < bytes.len() && bytes[*pos] == b':' {
        let prefix = input[start..*pos].to_string();
        *pos += 1; // skip ':'
        let local_start = *pos;
        while *pos < bytes.len() && is_pn_local_char(bytes[*pos]) {
            *pos += 1;
        }
        let local = input[local_start..*pos].to_string();
        return Ok(Token::PrefixedName { prefix, local });
    }

    let ident = &input[start..*pos];

    // Check for keyword.
    if let Some(kw) = Keyword::from_str_case_insensitive(ident) {
        Ok(Token::Keyword(kw))
    } else {
        // Not a keyword; check if it could be a prefix with empty local part.
        // In SPARQL, bare identifiers that aren't keywords are unusual in most
        // positions. Treat as a prefixed name with no colon (error in parser).
        // Actually, we should check for the colon pattern — but we already
        // checked above and didn't find one. This is a bare identifier.
        // For now, return it as a PrefixedName with empty local to handle
        // cases like `rdf:type` where `rdf` is not a keyword.
        // Actually: if there's no colon, it's not a prefixed name. The only
        // bare identifiers in SPARQL are keywords. Return an error for
        // unknown bare identifiers, or better — treat as a keyword we might
        // have missed. For robustness, return as a PrefixedName with empty prefix.
        //
        // NOTE: In SPARQL, `a` is a keyword for rdf:type, and all other
        // bare names should be keywords. If we get here, it might be
        // a function name or type we haven't registered.
        Ok(Token::PrefixedName {
            prefix: String::new(),
            local: ident.to_string(),
        })
    }
}

/// Check if a byte is a valid start character for a SPARQL variable name.
fn is_varname_start(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Check if a byte is a valid continuation character for a SPARQL variable name.
fn is_varname_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'.'
}

/// Check if a byte is a valid PN_CHARS_BASE character (simplified to ASCII + underscore).
fn is_pn_chars_base(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

/// Check if a byte is a valid PN_CHARS character (for prefixed names and keywords).
fn is_pn_chars(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

/// Check if a byte is valid in the local part of a prefixed name.
fn is_pn_local_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b'.' || b == b'~' || b == b'%'
}

/// Advance past a single UTF-8 character and return it.
fn next_char(input: &str, pos: &mut usize) -> char {
    let ch = input[*pos..].chars().next().unwrap();
    *pos += ch.len_utf8();
    ch
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lex_simple_select() {
        let input = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1].kind, Token::Variable("s".to_string()));
        assert_eq!(tokens[2].kind, Token::Variable("p".to_string()));
        assert_eq!(tokens[3].kind, Token::Variable("o".to_string()));
        assert_eq!(tokens[4].kind, Token::Keyword(Keyword::Where));
        assert_eq!(tokens[5].kind, Token::LBrace);
        assert_eq!(tokens[6].kind, Token::Variable("s".to_string()));
        assert_eq!(tokens[7].kind, Token::Variable("p".to_string()));
        assert_eq!(tokens[8].kind, Token::Variable("o".to_string()));
        assert_eq!(tokens[9].kind, Token::RBrace);
        assert_eq!(tokens.len(), 10);
    }

    #[test]
    fn test_lex_prefix_and_iri() {
        let input = r#"PREFIX ex: <http://example.org/>
SELECT ?s WHERE { ?s ex:name "Alice" }"#;
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Prefix));
        assert_eq!(
            tokens[1].kind,
            Token::PrefixedName {
                prefix: "ex".to_string(),
                local: String::new(),
            }
        );
        assert_eq!(
            tokens[2].kind,
            Token::Iri("http://example.org/".to_string())
        );
        assert_eq!(tokens[3].kind, Token::Keyword(Keyword::Select));
        assert_eq!(tokens[4].kind, Token::Variable("s".to_string()));
        assert_eq!(tokens[5].kind, Token::Keyword(Keyword::Where));
        assert_eq!(tokens[6].kind, Token::LBrace);
        assert_eq!(tokens[7].kind, Token::Variable("s".to_string()));
        assert_eq!(
            tokens[8].kind,
            Token::PrefixedName {
                prefix: "ex".to_string(),
                local: "name".to_string(),
            }
        );
        assert_eq!(tokens[9].kind, Token::StringLiteral("Alice".to_string()));
        assert_eq!(tokens[10].kind, Token::RBrace);
    }

    #[test]
    fn test_lex_filter_with_operators() {
        let input = "FILTER(?age >= 18 && ?age < 65)";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Filter));
        assert_eq!(tokens[1].kind, Token::LParen);
        assert_eq!(tokens[2].kind, Token::Variable("age".to_string()));
        assert_eq!(tokens[3].kind, Token::GtEq);
        assert_eq!(tokens[4].kind, Token::Integer("18".to_string()));
        assert_eq!(tokens[5].kind, Token::AmpAmp);
        assert_eq!(tokens[6].kind, Token::Variable("age".to_string()));
        // `<` followed by space should be less-than, not start of IRI
        assert_eq!(tokens[7].kind, Token::Lt);
        assert_eq!(tokens[8].kind, Token::Integer("65".to_string()));
        assert_eq!(tokens[9].kind, Token::RParen);
    }

    #[test]
    fn test_lex_typed_literal() {
        let input = r#""42"^^<http://www.w3.org/2001/XMLSchema#integer>"#;
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::StringLiteral("42".to_string()));
        assert_eq!(tokens[1].kind, Token::DoubleCaret);
        assert_eq!(
            tokens[2].kind,
            Token::Iri("http://www.w3.org/2001/XMLSchema#integer".to_string())
        );
    }

    #[test]
    fn test_lex_language_tagged_literal() {
        let input = r#""chat"@fr"#;
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::StringLiteral("chat".to_string()));
        assert_eq!(tokens[1].kind, Token::LangTag("fr".to_string()));
    }

    #[test]
    fn test_lex_blank_node() {
        let input = "_:b0 ?p ?o";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::BlankNodeLabel("b0".to_string()));
        assert_eq!(tokens[1].kind, Token::Variable("p".to_string()));
        assert_eq!(tokens[2].kind, Token::Variable("o".to_string()));
    }

    #[test]
    fn test_lex_numeric_literals() {
        let input = "42 3.14 1.5e10";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Integer("42".to_string()));
        assert_eq!(tokens[1].kind, Token::Decimal("3.14".to_string()));
        assert_eq!(tokens[2].kind, Token::Double("1.5e10".to_string()));
    }

    #[test]
    fn test_lex_property_path_operators() {
        let input = "^ex:knows/ex:name|ex:label";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Caret);
        assert_eq!(
            tokens[1].kind,
            Token::PrefixedName {
                prefix: "ex".to_string(),
                local: "knows".to_string()
            }
        );
        assert_eq!(tokens[2].kind, Token::Slash);
        assert_eq!(
            tokens[3].kind,
            Token::PrefixedName {
                prefix: "ex".to_string(),
                local: "name".to_string()
            }
        );
        assert_eq!(tokens[4].kind, Token::Pipe);
        assert_eq!(
            tokens[5].kind,
            Token::PrefixedName {
                prefix: "ex".to_string(),
                local: "label".to_string()
            }
        );
    }

    #[test]
    fn test_lex_comment_skipping() {
        let input = "SELECT ?s # this is a comment\n?p ?o";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1].kind, Token::Variable("s".to_string()));
        assert_eq!(tokens[2].kind, Token::Variable("p".to_string()));
        assert_eq!(tokens[3].kind, Token::Variable("o".to_string()));
    }

    #[test]
    fn test_lex_case_insensitive_keywords() {
        let input = "select WHERE optional FILTER";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Select));
        assert_eq!(tokens[1].kind, Token::Keyword(Keyword::Where));
        assert_eq!(tokens[2].kind, Token::Keyword(Keyword::Optional));
        assert_eq!(tokens[3].kind, Token::Keyword(Keyword::Filter));
    }

    #[test]
    fn test_lex_full_query() {
        let input = r#"PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name ?email
WHERE {
  ?person foaf:name ?name .
  OPTIONAL { ?person foaf:mbox ?email }
  FILTER(?name != "Unknown")
}
ORDER BY ?name
LIMIT 10"#;
        let tokens = lex(input).unwrap();
        // Just verify it lexes without error and has reasonable token count.
        assert!(tokens.len() > 20);
        // Check first few tokens.
        assert_eq!(tokens[0].kind, Token::Keyword(Keyword::Prefix));
        // Check last token is a number.
        assert_eq!(
            tokens[tokens.len() - 1].kind,
            Token::Integer("10".to_string())
        );
    }

    #[test]
    fn test_lex_string_escapes() {
        let input = r#""hello\nworld""#;
        let tokens = lex(input).unwrap();
        assert_eq!(
            tokens[0].kind,
            Token::StringLiteral("hello\nworld".to_string())
        );
    }

    #[test]
    fn test_lex_empty_prefix() {
        // `:localName` — prefixed name with empty prefix.
        let input = ":foo";
        let tokens = lex(input).unwrap();
        assert_eq!(
            tokens[0].kind,
            Token::PrefixedName {
                prefix: String::new(),
                local: "foo".to_string(),
            }
        );

        // `PREFIX : <iri>` — declaration of the default prefix.
        let input2 = "PREFIX : <http://example.org/>";
        let tokens2 = lex(input2).unwrap();
        assert_eq!(tokens2[0].kind, Token::Keyword(Keyword::Prefix));
        assert_eq!(
            tokens2[1].kind,
            Token::PrefixedName {
                prefix: String::new(),
                local: String::new(),
            }
        );
        assert_eq!(
            tokens2[2].kind,
            Token::Iri("http://example.org/".to_string())
        );
    }

    #[test]
    fn test_lex_dollar_variable() {
        let input = "SELECT $x $y WHERE { $x ?p $y }";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[1].kind, Token::Variable("x".to_string()));
        assert_eq!(tokens[2].kind, Token::Variable("y".to_string()));
    }

    #[test]
    fn test_lex_offsets() {
        let input = "SELECT ?s";
        let tokens = lex(input).unwrap();
        assert_eq!(tokens[0].offset, 0);
        assert_eq!(tokens[1].offset, 7);
    }
}
