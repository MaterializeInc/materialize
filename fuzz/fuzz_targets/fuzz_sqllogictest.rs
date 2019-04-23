#![no_main]

#[macro_use]
extern crate libfuzzer_sys;

fuzz_target!(|data: &[u8]| {
    if let Ok(string) = std::str::from_utf8(data) {
        sqllogictest::run(string.to_owned());
    };
});
