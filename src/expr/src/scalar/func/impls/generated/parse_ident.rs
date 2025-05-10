// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


#[derive(
    proptest_derive::Arbitrary,
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    mz_lowertest::MzReflect
)]
pub struct ParseIdent;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for ParseIdent {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        a: Self::Input1,
        b: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            fn is_ident_start(c: char) -> bool {
                matches!(c, 'A'..='Z' | 'a'..='z' | '_' | '\u{80}'..= char::MAX)
            }
            fn is_ident_cont(c: char) -> bool {
                matches!(c, '0'..='9' | '$') || is_ident_start(c)
            }
            let ident = a.unwrap_str();
            let strict = b.unwrap_bool();
            let mut elems = vec![];
            let buf = &mut LexBuf::new(ident);
            let mut after_dot = false;
            buf.take_while(|ch| ch.is_ascii_whitespace());
            loop {
                let mut missing_ident = true;
                let c = buf.next();
                if c == Some('"') {
                    let s = buf.take_while(|ch| !matches!(ch, '"'));
                    if buf.next() != Some('"') {
                        return Err(EvalError::InvalidIdentifier {
                            ident: ident.into(),
                            detail: Some("String has unclosed double quotes.".into()),
                        });
                    }
                    elems.push(Datum::String(s));
                    missing_ident = false;
                } else if c.map(is_ident_start).unwrap_or(false) {
                    buf.prev();
                    let s = buf.take_while(is_ident_cont);
                    let s = temp_storage.push_string(s.to_ascii_lowercase());
                    elems.push(Datum::String(s));
                    missing_ident = false;
                }
                if missing_ident {
                    if c == Some('.') {
                        return Err(EvalError::InvalidIdentifier {
                            ident: ident.into(),
                            detail: Some("No valid identifier before \".\".".into()),
                        });
                    } else if after_dot {
                        return Err(EvalError::InvalidIdentifier {
                            ident: ident.into(),
                            detail: Some("No valid identifier after \".\".".into()),
                        });
                    } else {
                        return Err(EvalError::InvalidIdentifier {
                            ident: ident.into(),
                            detail: None,
                        });
                    }
                }
                buf.take_while(|ch| ch.is_ascii_whitespace());
                match buf.next() {
                    Some('.') => {
                        after_dot = true;
                        buf.take_while(|ch| ch.is_ascii_whitespace());
                    }
                    Some(_) if strict => {
                        return Err(EvalError::InvalidIdentifier {
                            ident: ident.into(),
                            detail: None,
                        });
                    }
                    _ => break,
                }
            }
            Ok(
                temp_storage
                    .try_make_datum(|packer| {
                        packer
                            .try_push_array(
                                &[
                                    ArrayDimension {
                                        lower_bound: 1,
                                        length: elems.len(),
                                    },
                                ],
                                elems,
                            )
                    })?,
            )
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <mz_repr::ArrayRustType<String>>::as_column_type();
        let propagates_nulls = crate::func::binary::EagerBinaryFunc::propagates_nulls(
            self,
        );
        let nullable = output.nullable;
        output
            .nullable(
                nullable
                    || (propagates_nulls
                        && (input_type_a.nullable || input_type_b.nullable)),
            )
    }
    fn introduces_nulls(&self) -> bool {
        <mz_repr::ArrayRustType<String> as ::mz_repr::DatumType<'_, ()>>::nullable()
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for ParseIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(parse_ident))
    }
}
