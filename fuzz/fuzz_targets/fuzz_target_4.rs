#![no_main]

use mz_lowertest::{deserialize_optional_generic, tokenize};
use mz_repr_test_util::*;
use mz_repr::Row;
use libfuzzer_sys::fuzz_target;

fn f(s: &str) -> Result<Row, String> {
    let mut stream_iter = tokenize(s)?.into_iter();
    if let Some(litval) =
         extract_literal_string(&stream_iter.next().ok_or("Empty test")?, &mut stream_iter)? {
        let scalar_type = get_scalar_type_or_default(&litval[..], &mut stream_iter)?;
        mz_repr_test_util::test_spec_to_row(std::iter::once((&litval[..], &scalar_type)))
    } else {
        Err("foo".to_string())
    }
}

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = f(s);
    }
});
