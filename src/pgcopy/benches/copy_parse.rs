// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for COPY text parsing: memchr-accelerated vs byte-by-byte scanning.

use criterion::{Criterion, criterion_group, criterion_main};
use mz_pgcopy::CopyTextFormatParser;
use std::hint::black_box;
use std::io;

// ============================================================
// Old byte-by-byte parser (pre-memchr) for comparison
// ============================================================
struct OldParser<'a> {
    data: &'a [u8],
    position: usize,
    column_delimiter: u8,
    null_string: &'a str,
    buffer: Vec<u8>,
}

static END_OF_COPY_MARKER: &[u8] = b"\\.";

impl<'a> OldParser<'a> {
    fn new(data: &'a [u8], column_delimiter: u8, null_string: &'a str) -> Self {
        Self {
            data,
            position: 0,
            column_delimiter,
            null_string,
            buffer: Vec::new(),
        }
    }

    fn peek(&self) -> Option<u8> {
        if self.position < self.data.len() {
            Some(self.data[self.position])
        } else {
            None
        }
    }

    fn consume_n(&mut self, n: usize) {
        self.position = std::cmp::min(self.position + n, self.data.len());
    }

    fn is_eof(&self) -> bool {
        self.peek().is_none() || self.is_end_of_copy_marker()
    }

    fn is_end_of_copy_marker(&self) -> bool {
        self.check_bytes(END_OF_COPY_MARKER)
    }

    fn is_end_of_line(&self) -> bool {
        match self.peek() {
            Some(b'\n') | None => true,
            _ => false,
        }
    }

    fn expect_end_of_line(&mut self) -> Result<(), io::Error> {
        if self.is_end_of_line() {
            self.consume_n(1);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "extra data after last expected column",
            ))
        }
    }

    fn is_column_delimiter(&self) -> bool {
        self.check_bytes(&[self.column_delimiter])
    }

    fn expect_column_delimiter(&mut self) -> Result<(), io::Error> {
        if self.check_bytes(&[self.column_delimiter]) {
            self.consume_n(1);
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing data for column",
            ))
        }
    }

    fn check_bytes(&self, bytes: &[u8]) -> bool {
        self.data
            .get(self.position..self.position + bytes.len())
            .map_or(false, |d| d == bytes)
    }

    fn consume_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.check_bytes(bytes) {
            self.consume_n(bytes.len());
            true
        } else {
            false
        }
    }

    fn consume_null_string(&mut self) -> bool {
        if self.null_string.is_empty() {
            self.is_column_delimiter()
                || self.is_end_of_line()
                || self.is_end_of_copy_marker()
                || self.is_eof()
        } else {
            self.consume_bytes(self.null_string.as_bytes())
        }
    }

    fn consume_raw_value(&mut self) -> Result<Option<&[u8]>, io::Error> {
        if self.consume_null_string() {
            return Ok(None);
        }

        let mut start = self.position;
        self.buffer.clear();

        while !self.is_eof() && !self.is_end_of_copy_marker() {
            if self.is_end_of_line() || self.is_column_delimiter() {
                break;
            }
            match self.peek() {
                Some(b'\\') => {
                    self.buffer.extend(&self.data[start..self.position]);
                    self.consume_n(1);
                    match self.peek() {
                        Some(b'b') => {
                            self.consume_n(1);
                            self.buffer.push(8);
                        }
                        Some(b'f') => {
                            self.consume_n(1);
                            self.buffer.push(12);
                        }
                        Some(b'n') => {
                            self.consume_n(1);
                            self.buffer.push(b'\n');
                        }
                        Some(b'r') => {
                            self.consume_n(1);
                            self.buffer.push(b'\r');
                        }
                        Some(b't') => {
                            self.consume_n(1);
                            self.buffer.push(b'\t');
                        }
                        Some(b'v') => {
                            self.consume_n(1);
                            self.buffer.push(11);
                        }
                        Some(b'x') => {
                            self.consume_n(1);
                            match self.peek() {
                                Some(_c @ b'0'..=b'9')
                                | Some(_c @ b'A'..=b'F')
                                | Some(_c @ b'a'..=b'f') => {
                                    let mut value: u8 = 0;
                                    let decode_nibble = |b| match b {
                                        Some(c @ b'a'..=b'f') => Some(c - b'a' + 10),
                                        Some(c @ b'A'..=b'F') => Some(c - b'A' + 10),
                                        Some(c @ b'0'..=b'9') => Some(c - b'0'),
                                        _ => None,
                                    };
                                    for _ in 0..2 {
                                        match decode_nibble(self.peek()) {
                                            Some(c) => {
                                                self.consume_n(1);
                                                value = value << 4 | c;
                                            }
                                            _ => break,
                                        }
                                    }
                                    self.buffer.push(value);
                                }
                                _ => {
                                    self.buffer.push(b'x');
                                }
                            }
                        }
                        Some(_c @ b'0'..=b'7') => {
                            let mut value: u8 = 0;
                            for _ in 0..3 {
                                match self.peek() {
                                    Some(c @ b'0'..=b'7') => {
                                        self.consume_n(1);
                                        value = value << 3 | (c - b'0');
                                    }
                                    _ => break,
                                }
                            }
                            self.buffer.push(value);
                        }
                        Some(c) => {
                            self.consume_n(1);
                            self.buffer.push(c);
                        }
                        None => {
                            self.buffer.push(b'\\');
                        }
                    }
                    start = self.position;
                }
                Some(_) => {
                    self.consume_n(1);
                }
                None => {}
            }
        }

        if self.buffer.is_empty() {
            Ok(Some(&self.data[start..self.position]))
        } else {
            self.buffer.extend(&self.data[start..self.position]);
            Ok(Some(&self.buffer[..]))
        }
    }
}

// ============================================================
// Data generators
// ============================================================

/// Generate COPY text format data with integer values.
fn gen_integers_data(num_rows: usize, num_cols: usize) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        for c in 0..num_cols {
            if c > 0 {
                data.push(b'\t');
            }
            data.extend(format!("{}", (i * num_cols + c) as i64 * 42).as_bytes());
        }
        data.push(b'\n');
    }
    data.extend(b"\\.\n");
    data
}

/// Generate COPY text format data with string values (no escaping needed).
fn gen_strings_data(num_rows: usize, num_cols: usize) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        for c in 0..num_cols {
            if c > 0 {
                data.push(b'\t');
            }
            data.extend(format!("row_{}_col_{}_value", i, c).as_bytes());
        }
        data.push(b'\n');
    }
    data.extend(b"\\.\n");
    data
}

/// Generate COPY text format data with long string values (no escaping).
fn gen_long_strings_data(num_rows: usize, num_cols: usize) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        for c in 0..num_cols {
            if c > 0 {
                data.push(b'\t');
            }
            data.extend(
                format!(
                    "this is a longer string value for row {} column {} with some padding data here",
                    i, c
                )
                .as_bytes(),
            );
        }
        data.push(b'\n');
    }
    data.extend(b"\\.\n");
    data
}

/// Generate COPY text format data with mixed values.
fn gen_mixed_data(num_rows: usize) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        data.extend(format!("{}", i).as_bytes());
        data.push(b'\t');
        data.extend(format!("hello_world_{}", i).as_bytes());
        data.push(b'\t');
        data.extend(if i % 2 == 0 { b"t" } else { b"f" });
        data.push(b'\t');
        data.extend(format!("{}", i as i64 * 1000).as_bytes());
        data.push(b'\t');
        data.extend(b"test_value");
        data.push(b'\n');
    }
    data.extend(b"\\.\n");
    data
}

/// Generate COPY text format data with escaped characters.
fn gen_escaped_data(num_rows: usize) -> Vec<u8> {
    let mut data = Vec::new();
    for i in 0..num_rows {
        data.extend(format!("{}", i).as_bytes());
        data.push(b'\t');
        data.extend(format!("path\\\\to\\\\file_{}", i).as_bytes());
        data.push(b'\t');
        data.extend(format!("line1\\nline2_{}", i).as_bytes());
        data.push(b'\t');
        data.extend(b"no_escapes_here");
        data.push(b'\n');
    }
    data.extend(b"\\.\n");
    data
}

// ============================================================
// Parsing helpers
// ============================================================

/// Parse all values from COPY text data using new memchr-accelerated parser.
fn parse_all_new(data: &[u8]) -> usize {
    let mut parser = CopyTextFormatParser::new(data, b'\t', "\\N");
    let mut count = 0;
    while !parser.is_eof() {
        loop {
            match parser.consume_raw_value() {
                Ok(Some(_)) => count += 1,
                Ok(None) => count += 1,
                Err(_) => break,
            }
            if parser.is_eof() {
                break;
            }
            match parser.expect_end_of_line() {
                Ok(()) => break,
                Err(_) => {
                    let _ = parser.expect_column_delimiter();
                }
            }
        }
    }
    count
}

/// Parse all values from COPY text data using old byte-by-byte parser.
fn parse_all_old(data: &[u8]) -> usize {
    let mut parser = OldParser::new(data, b'\t', "\\N");
    let mut count = 0;
    while !parser.is_eof() {
        loop {
            match parser.consume_raw_value() {
                Ok(Some(_)) => count += 1,
                Ok(None) => count += 1,
                Err(_) => break,
            }
            if parser.is_eof() {
                break;
            }
            match parser.expect_end_of_line() {
                Ok(()) => break,
                Err(_) => {
                    let _ = parser.expect_column_delimiter();
                }
            }
        }
    }
    count
}

// ============================================================
// Benchmarks
// ============================================================

fn bench_parse_integers(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_int5");
    let data = gen_integers_data(10_000, 5);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.finish();
}

fn bench_parse_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_str5");
    let data = gen_strings_data(10_000, 5);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.finish();
}

fn bench_parse_long_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_longstr5");
    let data = gen_long_strings_data(10_000, 5);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.finish();
}

fn bench_parse_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_mixed5");
    let data = gen_mixed_data(10_000);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 50_000);
            black_box(count)
        })
    });
    group.finish();
}

fn bench_parse_escaped(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_escaped");
    let data = gen_escaped_data(10_000);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 40_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 40_000);
            black_box(count)
        })
    });
    group.finish();
}

fn bench_parse_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_wide20");
    let data = gen_integers_data(10_000, 20);

    group.bench_function("old_byte_by_byte", |b| {
        b.iter(|| {
            let count = parse_all_old(black_box(&data));
            assert_eq!(count, 200_000);
            black_box(count)
        })
    });
    group.bench_function("new_memchr", |b| {
        b.iter(|| {
            let count = parse_all_new(black_box(&data));
            assert_eq!(count, 200_000);
            black_box(count)
        })
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_parse_integers,
    bench_parse_strings,
    bench_parse_long_strings,
    bench_parse_mixed,
    bench_parse_escaped,
    bench_parse_wide,
);
criterion_main!(benches);
