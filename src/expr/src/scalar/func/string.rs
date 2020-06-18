// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! String and byte string functions.

use std::cmp;
use std::convert::TryFrom;
use std::str;

use encoding::label::encoding_from_whatwg_label;
use encoding::DecoderTrap;

use repr::strconv;
use repr::{Datum, RowArena, ScalarType};

use crate::like_pattern;
use crate::scalar::func::{FuncProps, Nulls, OutputType};
use crate::scalar::EvalError;

pub const CAST_BYTES_TO_STRING_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn cast_bytes_to_string<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    strconv::format_bytes(&mut buf, a.unwrap_bytes());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CAST_STRING_TO_BYTES_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: true,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bytes),
};

pub fn cast_string_to_bytes<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let bytes = strconv::parse_bytes(a.unwrap_str())?;
    Ok(Datum::Bytes(temp_storage.push_bytes(bytes)))
}

pub const CONVERT_FROM_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn convert_from<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace("_", "-");

    match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => {
            // todo@jldlaughlin: #2282
            if enc.name() != "utf-8" {
                return Err(EvalError::InvalidEncodingName(encoding_name));
            }
        }
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    }

    let bytes = a.unwrap_bytes();
    match str::from_utf8(bytes) {
        Ok(from) => Ok(Datum::String(from)),
        Err(e) => {
            let mut byte_sequence = String::new();
            strconv::format_bytes(&mut byte_sequence, &bytes);
            Err(EvalError::InvalidByteSequence {
                byte_sequence: e.to_string(),
                encoding_name,
            })
        }
    }
}

pub const LENGTH_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn bit_length<B>(bytes: B) -> Result<Datum<'static>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len() * 8) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

pub fn byte_length<B>(bytes: B) -> Result<Datum<'static>, EvalError>
where
    B: AsRef<[u8]>,
{
    match i32::try_from(bytes.as_ref().len()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

pub fn char_length(a: Datum) -> Result<Datum, EvalError> {
    match i32::try_from(a.unwrap_str().chars().count()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

pub fn encoded_bytes_char_length<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    // Convert PostgreSQL-style encoding names[1] to WHATWG-style encoding names[2],
    // which the encoding library uses[3].
    // [1]: https://www.postgresql.org/docs/9.5/multibyte.html
    // [2]: https://encoding.spec.whatwg.org/
    // [3]: https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs
    let encoding_name = b.unwrap_str().to_lowercase().replace("_", "-");

    let enc = match encoding_from_whatwg_label(&encoding_name) {
        Some(enc) => enc,
        None => return Err(EvalError::InvalidEncodingName(encoding_name)),
    };

    let decoded_string = match enc.decode(a.unwrap_bytes(), DecoderTrap::Strict) {
        Ok(s) => s,
        Err(e) => {
            return Err(EvalError::InvalidByteSequence {
                byte_sequence: e.to_string(),
                encoding_name,
            })
        }
    };

    match i32::try_from(decoded_string.chars().count()) {
        Ok(l) => Ok(Datum::from(l)),
        Err(_) => Err(EvalError::IntegerOutOfRange),
    }
}

pub const ASCII_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Int32),
};

pub fn ascii(a: Datum) -> Result<Datum, EvalError> {
    match a.unwrap_str().chars().next() {
        None => Ok(Datum::Int32(0)),
        Some(v) => Ok(Datum::Int32(v as i32)),
    }
}

pub const CONCAT_BINARY_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn concat_binary<'a>(
    a: Datum<'a>,
    b: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    buf.push_str(a.unwrap_str());
    buf.push_str(b.unwrap_str());
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const CONCAT_VARIADIC_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Never,
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn concat_variadic<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    let mut buf = String::new();
    for d in datums {
        if !d.is_null() {
            buf.push_str(d.unwrap_str());
        }
    }
    Ok(Datum::String(temp_storage.push_string(buf)))
}

pub const TRIM_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: false,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn trim_whitespace(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_str().trim_matches(' ')))
}

pub fn trim<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let trim_chars = b.unwrap_str();

    Ok(Datum::from(
        a.unwrap_str().trim_matches(|c| trim_chars.contains(c)),
    ))
}

pub fn trim_leading_whitespace(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_str().trim_start_matches(' ')))
}

pub fn trim_leading<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let trim_chars = b.unwrap_str();

    Ok(Datum::from(
        a.unwrap_str()
            .trim_start_matches(|c| trim_chars.contains(c)),
    ))
}

pub fn trim_trailing_whitespace(a: Datum) -> Result<Datum, EvalError> {
    Ok(Datum::from(a.unwrap_str().trim_end_matches(' ')))
}

pub fn trim_trailing<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let trim_chars = b.unwrap_str();

    Ok(Datum::from(
        a.unwrap_str().trim_end_matches(|c| trim_chars.contains(c)),
    ))
}

pub const SUBSTR_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: true, // TODO(benesch): should error instead
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn substr<'a>(datums: &[Datum<'a>]) -> Result<Datum<'a>, EvalError> {
    let string: &'a str = datums[0].unwrap_str();
    let mut chars = string.chars();

    let start_in_chars = datums[1].unwrap_int64() - 1;
    let mut start_in_bytes = 0;
    for _ in 0..cmp::max(start_in_chars, 0) {
        start_in_bytes += chars.next().map(|char| char.len_utf8()).unwrap_or(0);
    }

    if datums.len() == 3 {
        let mut length_in_chars = datums[2].unwrap_int64();
        if length_in_chars < 0 {
            return Ok(Datum::Null);
        }
        if start_in_chars < 0 {
            length_in_chars += start_in_chars;
        }
        let mut length_in_bytes = 0;
        for _ in 0..cmp::max(length_in_chars, 0) {
            length_in_bytes += chars.next().map(|char| char.len_utf8()).unwrap_or(0);
        }
        Ok(Datum::String(
            &string[start_in_bytes..start_in_bytes + length_in_bytes],
        ))
    } else {
        Ok(Datum::String(&string[start_in_bytes..]))
    }
}

pub const REPLACE_PROPS: FuncProps = FuncProps {
    can_error: false,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::String),
};

pub fn replace<'a>(
    datums: &[Datum<'a>],
    temp_storage: &'a RowArena,
) -> Result<Datum<'a>, EvalError> {
    Ok(Datum::String(
        temp_storage.push_string(
            datums[0]
                .unwrap_str()
                .replace(datums[1].unwrap_str(), datums[2].unwrap_str()),
        ),
    ))
}

pub const MATCH_LIKE_PATTERN_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn match_like_pattern<'a>(a: Datum<'a>, b: Datum<'a>) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    let needle = like_pattern::build_regex(b.unwrap_str())?;
    Ok(Datum::from(needle.is_match(haystack)))
}

pub const MATCH_REGEX_PROPS: FuncProps = FuncProps {
    can_error: true,
    preserves_uniqueness: false,
    nulls: Nulls::Sometimes {
        propagates_nulls: true,
        introduces_nulls: false,
    },
    output_type: OutputType::Fixed(ScalarType::Bool),
};

pub fn match_regex<'a>(a: Datum<'a>, needle: &regex::Regex) -> Result<Datum<'a>, EvalError> {
    let haystack = a.unwrap_str();
    Ok(Datum::from(needle.is_match(haystack)))
}
