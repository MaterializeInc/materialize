use chrono::NaiveDate;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use repr::{Datum, RowPacker};
use uuid::Uuid;

/// A type similar to [`Datum`] that can be proptest-generated.
#[derive(Debug, PartialEq, Clone, Arbitrary)]
enum PropertizedDatum {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),

    // TODO:
    // #[proptest()]
    // Date(NaiveDate),
    // Time(NaiveTime),
    // Timestamp(NaiveDateTime),
    // TimestampTz(DateTime<Utc>),
    // Interval(Interval),
    // Decimal(Significand),
    Bytes(Vec<u8>),
    String(String),
    // TODO:
    // Array(Array<'a>),
    // List(DatumList<'a>),
    // Map(DatumMap<'a>),
    JsonNull,
    // TODO:
    // Uuid(Uuid),
    Dummy,
}

impl<'a> Into<Datum<'a>> for &'a PropertizedDatum {
    fn into(self) -> Datum<'a> {
        use PropertizedDatum::*;
        match self {
            Null => Datum::Null,
            Bool(b) => Datum::from(*b),
            Int32(i) => Datum::from(*i),
            Int64(i) => Datum::from(*i),
            Float32(f) => Datum::from(*f),
            Float64(f) => Datum::from(*f),
            Bytes(b) => Datum::from(&b[..]),
            String(s) => Datum::from(s.as_str()),
            JsonNull => Datum::JsonNull,
            Dummy => Datum::Dummy,
        }
    }
}

proptest! {
    #[test]
    fn row_packing_roundtrips_single_valued(prop_datums in prop::collection::vec(any::<PropertizedDatum>(), 1..100)) {
        let mut packer = RowPacker::new();
        let datums: Vec<Datum<'_>> = prop_datums.iter().map(|pd| pd.into()).collect();
        for d in datums.iter() {
            packer.push(d.clone());
        }
        let row = packer.finish();
        let unpacked = row.unpack();
        assert_eq!(datums, unpacked);
    }
}
