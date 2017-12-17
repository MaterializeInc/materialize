#![feature(slice_patterns)]

extern crate failure;
extern crate serde_json;

pub mod encode;
pub mod schema;
pub mod types;
mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
