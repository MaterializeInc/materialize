#![feature(slice_patterns)]

extern crate failure;
extern crate libflate;
extern crate rand;
#[macro_use] extern crate serde;
extern crate serde_json;
#[cfg(feature = "snappy")] extern crate snap;

pub mod codec;
pub mod de;
pub mod schema;
pub mod ser;
pub mod types;
mod util;

pub use codec::Codec;

pub use de::de::from_value;
pub use de::reader;
pub use ser::writer;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
