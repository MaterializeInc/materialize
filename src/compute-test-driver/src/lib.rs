//! Headless frontend to `clusterd` for scripted compute tests.
//!
//! See `doc/developer/design/20260612_headless_compute_test_driver.md`.

pub mod data;
pub mod persist_host;

#[cfg(test)]
mod tests {
    #[mz_ore::test]
    fn it_links() {
        assert_eq!(2 + 2, 4);
    }
}
