#![feature(slice_patterns)]

extern crate failure;
extern crate libflate;
extern crate rand;
extern crate serde;
extern crate serde_json;
#[cfg(feature = "snappy")] extern crate snap;

pub mod decode;
pub mod encode;
pub mod reader;
pub mod schema;
pub mod types;
pub mod writer;
mod util;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Codec {
    Null,
    Deflate,
    #[cfg(feature = "snappy")] Snappy,
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
