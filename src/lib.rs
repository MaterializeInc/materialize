#![feature(slice_patterns)]

extern crate failure;
extern crate libflate;
extern crate rand;
#[macro_use] extern crate serde;
extern crate serde_json;
#[cfg(feature = "snappy")] extern crate snap;

pub mod codec;
pub mod de;
pub mod decode;
pub mod encode;
pub mod reader;
pub mod schema;
pub mod ser;
pub mod types;
pub mod writer;
mod util;

pub use codec::Codec;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
