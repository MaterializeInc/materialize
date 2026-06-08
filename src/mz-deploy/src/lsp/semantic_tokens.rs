// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP semantic tokens handler.
//!
//! Implements `textDocument/semanticTokens/full` by lexing the document with
//! [`mz_sql_lexer::lexer::lex`] and mapping each token to a standard LSP
//! token type. Comments (discarded by the lexer) are recovered via a
//! separate pre-scan that is aware of strings and quoted identifiers.
//!
//! The output is delta-encoded per LSP 3.16: tokens are sorted by byte
//! offset, split across line boundaries (LSP tokens are line-local), and
//! serialized as a flat `[deltaLine, deltaStartChar, length, tokenType, 0]`
//! sequence.
//!
//! Legend indices must match the order declared in the server's
//! [`SemanticTokensLegend`] (see [`legend_token_types`]).

use mz_sql_lexer::lexer::{self, PosToken, Token};
use tower_lsp::lsp_types::{SemanticToken, SemanticTokenType};

const TOKEN_TYPE_KEYWORD: u32 = 0;
const TOKEN_TYPE_STRING: u32 = 1;
const TOKEN_TYPE_NUMBER: u32 = 2;
const TOKEN_TYPE_OPERATOR: u32 = 3;
const TOKEN_TYPE_VARIABLE: u32 = 4;
const TOKEN_TYPE_PARAMETER: u32 = 5;
const TOKEN_TYPE_COMMENT: u32 = 6;

/// Token types in the order required for legend indices.
pub(super) fn legend_token_types() -> Vec<SemanticTokenType> {
    vec![
        SemanticTokenType::KEYWORD,
        SemanticTokenType::STRING,
        SemanticTokenType::NUMBER,
        SemanticTokenType::OPERATOR,
        SemanticTokenType::VARIABLE,
        SemanticTokenType::PARAMETER,
        SemanticTokenType::COMMENT,
    ]
}

/// Byte-offset span with an associated semantic token type.
#[derive(Debug, Clone, Copy)]
struct RawSpan {
    start: usize,
    end: usize,
    token_type: u32,
}

/// A line-local semantic token, after multi-line splitting.
#[derive(Debug, Clone, Copy)]
struct LineToken {
    line: u32,
    start_char: u32,
    length: u32,
    token_type: u32,
}

/// Computes the semantic tokens for a SQL document.
///
/// Produces a delta-encoded `Vec<SemanticToken>` suitable for returning in
/// a `SemanticTokensResult::Tokens`. Returns an empty vec on empty input.
/// On lexer error, emits the tokens collected up to the error site plus
/// comments from the full pre-scan; never panics.
pub(super) fn compute_semantic_tokens(text: &str) -> Vec<SemanticToken> {
    let mut spans = Vec::new();
    collect_comments(text, &mut spans);
    if let Ok(tokens) = lexer::lex(text) {
        for tok in &tokens {
            if let Some(span) = lex_token_span(tok, text) {
                spans.push(span);
            }
        }
    }
    spans.sort_by_key(|s| s.start);

    let line_tokens = split_across_lines(text, &spans);
    encode_deltas(&line_tokens)
}

/// Pre-scan raw text for `--` line comments and `/* */` block comments.
/// String bodies and quoted-identifier bodies are skipped so that comment
/// markers inside them are not misidentified.
fn collect_comments(text: &str, out: &mut Vec<RawSpan>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => i = skip_single_quoted(bytes, i),
            b'"' => i = skip_double_quoted(bytes, i),
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let start = i;
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                out.push(RawSpan {
                    start,
                    end: i,
                    token_type: TOKEN_TYPE_COMMENT,
                });
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let start = i;
                i += 2;
                let mut depth = 1usize;
                while i + 1 < bytes.len() && depth > 0 {
                    if bytes[i] == b'/' && bytes[i + 1] == b'*' {
                        depth += 1;
                        i += 2;
                    } else if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        depth -= 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                if depth > 0 {
                    i = bytes.len();
                }
                out.push(RawSpan {
                    start,
                    end: i,
                    token_type: TOKEN_TYPE_COMMENT,
                });
            }
            _ => i += 1,
        }
    }
}

/// Skip a `'...'` string body (with doubled-quote escape). Returns index
/// just past the closing quote, or `bytes.len()` if unterminated.
fn skip_single_quoted(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if bytes.get(i + 1) == Some(&b'\'') => i += 2,
            b'\'' => return i + 1,
            b'\\' if i + 1 < bytes.len() => i += 2,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Skip a `"..."` quoted-identifier body (with doubled-quote escape).
fn skip_double_quoted(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' if bytes.get(i + 1) == Some(&b'"') => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    bytes.len()
}

/// Map a lexer token to its byte span and semantic type.
///
/// Returns `None` for tokens that should not be highlighted (punctuation).
fn lex_token_span(tok: &PosToken, text: &str) -> Option<RawSpan> {
    let start = tok.offset;
    let bytes = text.as_bytes();
    let (end, token_type) = match &tok.kind {
        Token::Keyword(kw) => (start + kw.as_str().len(), TOKEN_TYPE_KEYWORD),
        Token::Op(op) => (start + op.len(), TOKEN_TYPE_OPERATOR),
        Token::Number(n) => (start + n.len(), TOKEN_TYPE_NUMBER),
        Token::Star | Token::Eq | Token::Colon => (start + 1, TOKEN_TYPE_OPERATOR),
        Token::DoubleColon | Token::Arrow => (start + 2, TOKEN_TYPE_OPERATOR),
        Token::Ident(_) => (start + scan_ident_len(bytes, start), TOKEN_TYPE_VARIABLE),
        Token::String(_) => (
            start + scan_string_token_len(bytes, start),
            TOKEN_TYPE_STRING,
        ),
        Token::HexString(_) => (
            start + scan_hex_string_token_len(bytes, start),
            TOKEN_TYPE_STRING,
        ),
        Token::Parameter(_) => (
            start + scan_parameter_len(bytes, start),
            TOKEN_TYPE_PARAMETER,
        ),
        Token::LParen
        | Token::RParen
        | Token::LBracket
        | Token::RBracket
        | Token::Dot
        | Token::Comma
        | Token::Semicolon => return None,
    };
    Some(RawSpan {
        start,
        end,
        token_type,
    })
}

fn scan_ident_len(bytes: &[u8], start: usize) -> usize {
    if bytes.get(start) == Some(&b'"') {
        skip_double_quoted(bytes, start) - start
    } else {
        let mut i = start;
        while i < bytes.len() {
            let c = bytes[i];
            if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c >= 0x80 {
                i += 1;
            } else {
                break;
            }
        }
        i - start
    }
}

/// Length of a string token. May be a normal `'...'` or extended `E'...'` /
/// `e'...'` form (the E prefix is part of the token offset).
fn scan_string_token_len(bytes: &[u8], start: usize) -> usize {
    let quote_pos = if bytes.get(start) == Some(&b'\'') {
        start
    } else {
        // E-prefix extended string, or dollar-quoted $$...$$.
        // Handle dollar-quoted by seeking to the next `$`.
        if bytes.get(start) == Some(&b'$') {
            return scan_dollar_quoted_len(bytes, start);
        }
        start + 1
    };
    skip_single_quoted(bytes, quote_pos) - start
}

/// Length of a hex string token: `x'...'` or `X'...'`.
fn scan_hex_string_token_len(bytes: &[u8], start: usize) -> usize {
    let quote_pos = start + 1;
    skip_single_quoted(bytes, quote_pos) - start
}

/// Length of a `$tag$body$tag$` dollar-quoted string. Matches the outer
/// delimiter using its tag (possibly empty).
fn scan_dollar_quoted_len(bytes: &[u8], start: usize) -> usize {
    // Find first `$` that closes the tag.
    let mut i = start + 1;
    while i < bytes.len() && bytes[i] != b'$' {
        i += 1;
    }
    if i >= bytes.len() {
        return bytes.len() - start;
    }
    let tag = &bytes[start..=i]; // includes both $s
    i += 1;
    while i + tag.len() <= bytes.len() {
        if &bytes[i..i + tag.len()] == tag {
            return (i + tag.len()) - start;
        }
        i += 1;
    }
    bytes.len() - start
}

fn scan_parameter_len(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    i - start
}

/// Split each raw span across line boundaries and compute UTF-16 column
/// offsets. Produces line-local tokens, still in byte-order.
fn split_across_lines(text: &str, spans: &[RawSpan]) -> Vec<LineToken> {
    let line_starts = line_starts(text);
    let mut out = Vec::with_capacity(spans.len());
    for span in spans {
        if span.end <= span.start {
            continue;
        }
        let start_line = line_for_offset(&line_starts, span.start);
        let end_line = line_for_offset(&line_starts, span.end.saturating_sub(1));
        if start_line == end_line {
            let line_start = line_starts[start_line];
            let start_col = utf16_len(&text[line_start..span.start]);
            let length = utf16_len(&text[span.start..span.end]);
            if length > 0 {
                out.push(LineToken {
                    line: saturating_u32(start_line),
                    start_char: saturating_u32(start_col),
                    length: saturating_u32(length),
                    token_type: span.token_type,
                });
            }
        } else {
            // Multi-line: emit one token per line segment.
            for line in start_line..=end_line {
                let line_start = line_starts[line];
                let line_end = line_starts.get(line + 1).copied().unwrap_or(text.len());
                let seg_start = span.start.max(line_start);
                // Trim trailing newline from the segment so we don't emit
                // a token that spans into the next line's column 0.
                let seg_end_raw = span.end.min(line_end);
                let seg_end = trim_trailing_newline(text, line_start, seg_end_raw);
                if seg_end <= seg_start {
                    continue;
                }
                let start_col = utf16_len(&text[line_start..seg_start]);
                let length = utf16_len(&text[seg_start..seg_end]);
                if length > 0 {
                    out.push(LineToken {
                        line: saturating_u32(line),
                        start_char: saturating_u32(start_col),
                        length: saturating_u32(length),
                        token_type: span.token_type,
                    });
                }
            }
        }
    }
    out.sort_by(|a, b| a.line.cmp(&b.line).then(a.start_char.cmp(&b.start_char)));
    out
}

/// Trim a trailing `\n` or `\r\n` from a segment so it doesn't include the
/// line terminator.
fn trim_trailing_newline(text: &str, line_start: usize, end: usize) -> usize {
    let bytes = text.as_bytes();
    let mut e = end;
    if e > line_start && bytes.get(e - 1) == Some(&b'\n') {
        e -= 1;
        if e > line_start && bytes.get(e - 1) == Some(&b'\r') {
            e -= 1;
        }
    }
    e
}

/// Byte offsets of the start of each line (including line 0 at offset 0).
fn line_starts(text: &str) -> Vec<usize> {
    let mut v = vec![0];
    for (i, b) in text.bytes().enumerate() {
        if b == b'\n' {
            v.push(i + 1);
        }
    }
    v
}

/// Binary search for the line containing `offset`.
fn line_for_offset(line_starts: &[usize], offset: usize) -> usize {
    match line_starts.binary_search(&offset) {
        Ok(i) => i,
        Err(i) => i - 1,
    }
}

/// Number of UTF-16 code units in `s`. ASCII-only fast path returns the
/// byte length; non-ASCII walks chars and sums `len_utf16`.
fn utf16_len(s: &str) -> usize {
    if s.is_ascii() {
        return s.len();
    }
    s.chars().map(|c| c.len_utf16()).sum()
}

/// Convert a `usize` (line/column/length in the document) into the `u32` width
/// required by the LSP semantic-token wire format. No-op on values below
/// `u32::MAX`; saturates otherwise. LSP positions are specified to be `u32`,
/// so any document large enough to saturate is already unrepresentable.
fn saturating_u32(v: usize) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

/// Delta-encode line-local tokens per LSP 3.16.
fn encode_deltas(tokens: &[LineToken]) -> Vec<SemanticToken> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut prev_line: u32 = 0;
    let mut prev_char: u32 = 0;
    for t in tokens {
        let delta_line = t.line - prev_line;
        let delta_start = if delta_line == 0 {
            t.start_char - prev_char
        } else {
            t.start_char
        };
        out.push(SemanticToken {
            delta_line,
            delta_start,
            length: t.length,
            token_type: t.token_type,
            token_modifiers_bitset: 0,
        });
        prev_line = t.line;
        prev_char = t.start_char;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_ore::cast::CastFrom;

    /// Decode a delta-encoded token list into absolute
    /// `(line, start_char, length, token_type)` tuples for easier assertions.
    fn decode(tokens: &[SemanticToken]) -> Vec<(u32, u32, u32, u32)> {
        let mut out = Vec::new();
        let mut line = 0u32;
        let mut ch = 0u32;
        for t in tokens {
            line += t.delta_line;
            ch = if t.delta_line == 0 {
                ch + t.delta_start
            } else {
                t.delta_start
            };
            out.push((line, ch, t.length, t.token_type));
        }
        out
    }

    #[test]
    fn empty_input() {
        assert!(compute_semantic_tokens("").is_empty());
    }

    #[test]
    fn basic_select() {
        let sql = "SELECT 1 FROM t";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // SELECT
                (0, 7, 1, TOKEN_TYPE_NUMBER),    // 1
                (0, 9, 4, TOKEN_TYPE_KEYWORD),   // FROM
                (0, 14, 1, TOKEN_TYPE_VARIABLE), // t
            ]
        );
    }

    #[test]
    fn create_mv() {
        let sql = "CREATE MATERIALIZED VIEW foo AS SELECT 1";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // CREATE
                (0, 7, 12, TOKEN_TYPE_KEYWORD),  // MATERIALIZED
                (0, 20, 4, TOKEN_TYPE_KEYWORD),  // VIEW
                (0, 25, 3, TOKEN_TYPE_VARIABLE), // foo
                (0, 29, 2, TOKEN_TYPE_KEYWORD),  // AS
                (0, 32, 6, TOKEN_TYPE_KEYWORD),  // SELECT
                (0, 39, 1, TOKEN_TYPE_NUMBER),   // 1
            ]
        );
    }

    #[test]
    fn line_comment_mid_statement() {
        let sql = "SELECT -- hi\n1";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD), // SELECT
                (0, 7, 5, TOKEN_TYPE_COMMENT), // -- hi
                (1, 0, 1, TOKEN_TYPE_NUMBER),  // 1
            ]
        );
    }

    #[test]
    fn block_comment_single_line() {
        let sql = "SELECT /* x */ 1";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD), // SELECT
                (0, 7, 7, TOKEN_TYPE_COMMENT), // /* x */
                (0, 15, 1, TOKEN_TYPE_NUMBER), // 1
            ]
        );
    }

    #[test]
    fn block_comment_multiline() {
        let sql = "/*\na\nb\n*/";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 2, TOKEN_TYPE_COMMENT), // /*
                (1, 0, 1, TOKEN_TYPE_COMMENT), // a
                (2, 0, 1, TOKEN_TYPE_COMMENT), // b
                (3, 0, 2, TOKEN_TYPE_COMMENT), // */
            ]
        );
    }

    #[test]
    fn string_literal() {
        let sql = "SELECT 'hello'";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD), // SELECT
                (0, 7, 7, TOKEN_TYPE_STRING),  // 'hello'
            ]
        );
    }

    #[test]
    fn comment_markers_inside_string_not_detected() {
        let sql = "SELECT '--not a comment' FROM t";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // SELECT
                (0, 7, 17, TOKEN_TYPE_STRING),   // '--not a comment'
                (0, 25, 4, TOKEN_TYPE_KEYWORD),  // FROM
                (0, 30, 1, TOKEN_TYPE_VARIABLE), // t
            ]
        );
    }

    #[test]
    fn quoted_identifier() {
        let sql = r#"SELECT "My Col" FROM t"#;
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // SELECT
                (0, 7, 8, TOKEN_TYPE_VARIABLE),  // "My Col"
                (0, 16, 4, TOKEN_TYPE_KEYWORD),  // FROM
                (0, 21, 1, TOKEN_TYPE_VARIABLE), // t
            ]
        );
    }

    #[test]
    fn operators() {
        let sql = "SELECT 1 + 2 * 3";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // SELECT
                (0, 7, 1, TOKEN_TYPE_NUMBER),    // 1
                (0, 9, 1, TOKEN_TYPE_OPERATOR),  // +
                (0, 11, 1, TOKEN_TYPE_NUMBER),   // 2
                (0, 13, 1, TOKEN_TYPE_OPERATOR), // *
                (0, 15, 1, TOKEN_TYPE_NUMBER),   // 3
            ]
        );
    }

    #[test]
    fn parameter() {
        let sql = "SELECT $1 + $42";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),    // SELECT
                (0, 7, 2, TOKEN_TYPE_PARAMETER),  // $1
                (0, 10, 1, TOKEN_TYPE_OPERATOR),  // +
                (0, 12, 3, TOKEN_TYPE_PARAMETER), // $42
            ]
        );
    }

    #[test]
    fn punctuation_is_skipped() {
        let sql = "SELECT (a, b.c);";
        let decoded = decode(&compute_semantic_tokens(sql));
        // Parens, comma, dot, semicolon should not appear.
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),   // SELECT
                (0, 8, 1, TOKEN_TYPE_VARIABLE),  // a
                (0, 11, 1, TOKEN_TYPE_VARIABLE), // b
                (0, 13, 1, TOKEN_TYPE_VARIABLE), // c
            ]
        );
    }

    #[test]
    fn non_ascii_identifier() {
        // Japanese katakana "テーブル" (meaning "table"). Each char is one UTF-16 unit.
        let sql = "SELECT テーブル";
        let decoded = decode(&compute_semantic_tokens(sql));
        assert_eq!(
            decoded,
            vec![
                (0, 0, 6, TOKEN_TYPE_KEYWORD),  // SELECT
                (0, 7, 4, TOKEN_TYPE_VARIABLE), // テーブル (4 chars = 4 UTF-16 units)
            ]
        );
    }

    #[test]
    fn lex_error_does_not_panic() {
        // `@@@` is not a valid SQL start — lex() errors. We should still
        // return some comment data or an empty list without panicking.
        let sql = "-- leading comment\n@@@";
        let tokens = compute_semantic_tokens(sql);
        // At least the comment should survive.
        let decoded = decode(&tokens);
        assert!(
            decoded
                .iter()
                .any(|(_, _, _, ty)| *ty == TOKEN_TYPE_COMMENT),
            "expected the comment to be recovered even on lex error: {decoded:?}"
        );
    }

    #[test]
    fn legend_order_matches_constants() {
        let legend = legend_token_types();
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_KEYWORD)],
            SemanticTokenType::KEYWORD
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_STRING)],
            SemanticTokenType::STRING
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_NUMBER)],
            SemanticTokenType::NUMBER
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_OPERATOR)],
            SemanticTokenType::OPERATOR
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_VARIABLE)],
            SemanticTokenType::VARIABLE
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_PARAMETER)],
            SemanticTokenType::PARAMETER
        );
        assert_eq!(
            legend[usize::cast_from(TOKEN_TYPE_COMMENT)],
            SemanticTokenType::COMMENT
        );
    }
}
