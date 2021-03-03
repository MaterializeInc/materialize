use chrono::NaiveDate;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use repr::{adt::decimal::Significand, Datum, RowPacker};
use std::time::Duration;
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

    // TODO: these variants just need a strategy defined:
    #[proptest(strategy = "arb_date().prop_map(PropertizedDatum::Date)")]
    Date(NaiveDate),
    // Time(NaiveTime),
    // Timestamp(NaiveDateTime),
    // TimestampTz(DateTime<Utc>),
    // Interval(Interval),
    #[proptest(strategy = "arb_significand().prop_map(PropertizedDatum::Decimal)")]
    Decimal(Significand),

    Bytes(Vec<u8>),
    String(String),

    // TODO: these variants need reimplementation of the corresponding types:
    // Array(Array<'a>),
    // List(DatumList<'a>),
    // Map(DatumMap<'a>),
    JsonNull,

    #[proptest(value = "PropertizedDatum::Uuid(Uuid::nil())")]
    Uuid(Uuid),

    Dummy,
}

fn arb_date() -> BoxedStrategy<NaiveDate> {
    any::<i64>()
        .prop_map(|v| NaiveDate::from_ymd(2000, 01, 01) + chrono::Duration::nanoseconds(v))
        .boxed()
}

fn arb_significand() -> BoxedStrategy<Significand> {
    any::<i128>().prop_map(|v| Significand::new(v)).boxed()
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
            Date(d) => Datum::from(*d),
            Decimal(s) => Datum::from(*s),
            Bytes(b) => Datum::from(&b[..]),
            String(s) => Datum::from(s.as_str()),
            JsonNull => Datum::JsonNull,
            Uuid(u) => Datum::from(*u),
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
