//! Output primitives for mz-deploy.
//!
//! Follows standard CLI convention:
//! - Human-readable output → stderr (`human!`, `humanln!`)
//! - Machine-parseable data → stdout (`machine`)

/// Print human-readable output to stderr (like `eprint!`).
#[macro_export]
macro_rules! human {
    ($($arg:tt)*) => {
        eprint!($($arg)*)
    };
}

/// Print human-readable output to stderr with newline (like `eprintln!`).
#[macro_export]
macro_rules! humanln {
    () => { eprintln!() };
    ($($arg:tt)*) => {
        eprintln!($($arg)*)
    };
}

/// Write machine-parseable JSON to stdout.
pub fn machine(value: &impl serde::Serialize) {
    println!("{}", serde_json::to_string(value).unwrap());
}
