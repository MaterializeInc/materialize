use proptest::prelude::*;

use repr::Datum;

proptest! {
    #[test]
    fn generates_datum(s in "\\PC*") {
        Datum::from(s.as_str());
    }
}
