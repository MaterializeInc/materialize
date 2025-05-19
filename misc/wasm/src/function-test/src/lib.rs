mod ext {
    unsafe extern "C" {
        pub fn square(n: i64) -> i64;
    }
}

fn square(n: i64) -> i64 {
    unsafe { ext::square(n) }
}

#[unsafe(no_mangle)]
pub fn run(n: i64) -> i64 {
    square(n) + 1
}
