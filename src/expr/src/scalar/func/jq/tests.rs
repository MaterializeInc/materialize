// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{SqlScalarType, arb_datum_for_scalar};
use proptest::prelude::*;
use proptest::strategy::Union;

use super::*;

fn run(input: &str, program: &str) -> Result<Option<String>, String> {
    let input: Jsonb = input.parse().unwrap();
    let program = JqProgram::new(program).map_err(|e| format!("jq: {e}"))?;
    program
        .eval(input.as_ref())
        .map(|out| out.map(|out| out.to_string()))
        .map_err(|e| e.to_string())
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn jq_eval() {
    assert_eq!(
        run(r#"{"a": [1, 2.5, 3]}"#, ".a | map(. * 2)"),
        Ok(Some("[2,5,6]".into()))
    );
    assert_eq!(
        run(
            r#"{"b": 2, "a": 1}"#,
            r#"to_entries | map(.key) | join(",")"#
        ),
        Ok(Some(r#""a,b""#.into()))
    );
    assert_eq!(
        run(r#"{"x": 123456789012345678901234567890.123}"#, ".x"),
        Ok(Some("123456789012345678901234567890.123".into()))
    );
    assert_eq!(run("[1, 2, 3]", ".[] | select(. > 5)"), Ok(None));
    assert_eq!(
        run("[1, 2, 3]", "[.[] | select(. > 1)]"),
        Ok(Some("[2,3]".into()))
    );
    // Logging filters are kept, as no-ops.
    assert_eq!(run("[1, 2]", "debug"), Ok(Some("[1,2]".into())));
    for (program, error) in [
        (".[]", "jq: program produced more than one value"),
        (". +", "jq: expected term at end of program"),
        (
            r#"import "x" as x; ."#,
            "jq: cannot import x: module loading not supported",
        ),
        (r#"error("boom")"#, r#"jq: "boom""#),
        ("halt_error(3)", "jq: program halted with exit code 3"),
    ] {
        assert_eq!(run("[1, 2]", program), Err(error.into()), "{program}");
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn jq_withheld_filters_are_undefined() {
    let calls = [
        ("env", "env/0"),
        ("now", "now/0"),
        ("localtime", "localtime/0"),
        (r#"strflocaltime("%H")"#, "strflocaltime/1"),
        ("input", "input/0"),
        ("inputs", "inputs/0"),
        (r#"strptime("%H")"#, "strptime/1"),
    ];
    // The logging filters are withheld and then defined again as no-ops.
    let silenced = ["debug_empty", "stderr_empty"];
    let mut covered: Vec<_> = calls
        .iter()
        .map(|(_, name)| name.split('/').next().unwrap())
        .chain(silenced)
        .collect();
    covered.sort();
    let mut withheld = WITHHELD.to_vec();
    withheld.sort();
    assert_eq!(covered, withheld);

    for (program, name) in calls {
        assert_eq!(
            compile(program).err(),
            Some(format!("undefined filter {name}")),
            "{program}"
        );
    }
    assert!(compile("$ENV").is_err());
}

fn from_val(val: &Val) -> Result<Jsonb, String> {
    let mut row = Row::default();
    pack_val(&mut row.packer(), val, 0)?;
    Ok(Jsonb::from_row(row))
}

fn str_val(s: &[u8]) -> Val {
    Val::utf8_str(s.to_vec())
}

fn arb_jsonb() -> impl Strategy<Value = Jsonb> {
    arb_datum_for_scalar(SqlScalarType::Jsonb)
        .prop_map(|d| Jsonb::from_row(Row::pack_slice(&[Datum::from(&d)])))
}

/// Numbers in the full `numeric` range, including negatives, which
/// [`arb_jsonb`] does not generate.
fn arb_numeric_jsonb() -> impl Strategy<Value = Jsonb> {
    (any::<i128>(), -45i32..45).prop_filter_map("out of numeric range", |(m, e)| {
        let n = strconv::parse_numeric(&format!("{m}e{e}")).ok()?;
        Some(Jsonb::from_row(Row::pack_slice(&[Datum::Numeric(n)])))
    })
}

fn arb_input() -> impl Strategy<Value = Jsonb> {
    Union::new([arb_jsonb().boxed(), arb_numeric_jsonb().boxed()])
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn jq_val_roundtrip(input in arb_input()) {
        let val = to_val(input.as_ref().into_datum());
        prop_assert_eq!(from_val(&val), Ok(input));
    }
}

#[mz_ore::test]
fn jq_to_val_numbers() {
    let num = |text: &str| {
        let input: Jsonb = text.parse().unwrap();
        match to_val(input.as_ref().into_datum()) {
            Val::Num(n) => n,
            other => panic!("{text}: {other:?}"),
        }
    };
    assert!(matches!(num("-3"), Num::Int(-3)));
    assert!(matches!(
        num("123456789012345678901234567890"),
        Num::BigInt(_)
    ));
    // Decimals keep their text until arithmetic needs a float.
    assert!(matches!(num("2.5"), Num::Dec(d) if d.as_str() == "2.5"));
}

#[mz_ore::test]
fn jq_from_val_rejects_non_json() {
    let byte_str = Val::byte_str(b"x".to_vec());
    let cases = [
        Val::Num(Num::Float(f64::NAN)),
        Val::Num(Num::Float(f64::INFINITY)),
        byte_str.clone(),
        Val::obj([(Val::Null, Val::Null)].into_iter().collect()),
        // The first of two keys that are equal after invalid UTF-8 is
        // replaced is dropped, but its value must still be JSON.
        Val::obj(
            [(str_val(b"\x80"), byte_str), (str_val(b"\xff"), Val::Null)]
                .into_iter()
                .collect(),
        ),
    ];
    for val in cases {
        assert!(from_val(&val).is_err(), "{val:?}");
    }
}

#[mz_ore::test]
fn jq_from_val_duplicate_keys_last_wins() {
    let val = Val::obj(
        [
            (str_val(b"\x80"), Val::Bool(false)),
            (str_val(b"\xff"), Val::Bool(true)),
        ]
        .into_iter()
        .collect(),
    );
    assert_eq!(from_val(&val), Ok("{\"\u{fffd}\": true}".parse().unwrap()));
}

#[mz_ore::test]
fn jq_from_val_depth() {
    for (depth, ok) in [(MAX_OUTPUT_DEPTH - 1, true), (MAX_OUTPUT_DEPTH, false)] {
        let mut arr = Val::Null;
        let mut obj = Val::Null;
        for _ in 0..depth {
            arr = Val::Arr(jaq_json::Rc::new(vec![arr]));
            obj = Val::obj([(str_val(b"k"), obj)].into_iter().collect());
        }
        for val in [arr, obj] {
            assert_eq!(from_val(&val).is_ok(), ok, "depth {depth}");
        }
    }
}
