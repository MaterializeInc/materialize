// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

fn run(program: &str, input: &str) -> Result<Option<String>, String> {
    Program::compile(program)?.eval(input)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: wasmtime
fn jq_eval() {
    assert_eq!(run(".a + 1", r#"{"a": 1}"#), Ok(Some("2".into())));
    assert_eq!(run("empty", "null"), Ok(None));
    assert_eq!(
        run(".[]", "[1, 2]"),
        Err("program produced more than one value".into())
    );
    assert_eq!(
        run(". +", "null"),
        Err("expected term at end of program".into())
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: wasmtime
fn jq_limits() {
    for (program, error) in [
        ("last(repeat(.))", "program exceeded its evaluation budget"),
        ("def f: 1 + f; f", "program exceeded its stack limit"),
        (
            r#""x" * 300000000"#,
            "program exceeded its memory limit of 256 MiB",
        ),
    ] {
        let program = Program::compile(program).expect(program);
        assert_eq!(program.eval("null"), Err(error.into()), "{error}");
        // The instance that trapped is replaced, and the program still runs.
        assert!(program.guest.lock().unwrap().is_none());
    }
    let program = Program::compile(r#"if . then "x" * 300000000 else 1 end"#).unwrap();
    assert!(program.eval("true").is_err());
    assert_eq!(program.eval("false"), Ok(Some("1".into())));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: wasmtime
fn jq_deep_programs_exhaust_guest_stack() {
    let guest_stack_exhausted =
        |e: CallError| matches!(e, CallError::Trap(Some(Trap::MemoryOutOfBounds), _));
    // Deep nesting fails to compile.
    let nested = format!("{}.{}", "[".repeat(4000), "]".repeat(4000));
    let err = Guest::new(&nested)
        .err()
        .expect("nesting exhausts the stack");
    assert!(guest_stack_exhausted(err));
    // Deep recursion fails to evaluate.
    let mut guest = Guest::new("def f: 1 + f; f").ok().unwrap();
    let eval = guest.eval.clone();
    let err = guest.call(eval, b"null", EVAL_FUEL).err().unwrap();
    assert!(guest_stack_exhausted(err));
}
