#![no_main]

use mz_lowertest::tokenize;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let s = String::from_utf8_lossy(data);
    let _ = tokenize(&s);
});