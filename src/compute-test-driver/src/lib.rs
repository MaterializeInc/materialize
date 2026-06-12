//! Headless frontend to `clusterd` for scripted compute tests.
//!
//! See `doc/developer/design/20260612_headless_compute_test_driver.md`.

pub mod ctp;
pub mod data;
pub mod driver;
pub mod persist_host;
pub mod responses;
pub mod target;

#[cfg(test)]
mod tests {
    #[mz_ore::test]
    fn it_links() {
        assert_eq!(2 + 2, 4);
    }
}
