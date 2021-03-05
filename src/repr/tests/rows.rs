use chrono::TimeZone;
use proptest::prelude::*;
use proptest_derive::Arbitrary;
use repr::{
    adt::decimal::Significand,
    adt::{array::ArrayDimension, interval::Interval},
    Datum, Row, RowPacker,
};
use std::ops::Add;
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

    #[proptest(
        strategy = "add_arb_duration(chrono::NaiveDate::from_ymd(2000, 01, 01)).prop_map(PropertizedDatum::Date)"
    )]
    Date(chrono::NaiveDate),
    #[proptest(
        strategy = "add_arb_duration(chrono::NaiveTime::from_hms(0, 0, 0)).prop_map(PropertizedDatum::Time)"
    )]
    Time(chrono::NaiveTime),
    #[proptest(
        strategy = "add_arb_duration(chrono::NaiveDateTime::from_timestamp(0, 0)).prop_map(PropertizedDatum::Timestamp)"
    )]
    Timestamp(chrono::NaiveDateTime),
    #[proptest(
        strategy = "add_arb_duration(chrono::Utc.timestamp(0, 0)).prop_map(PropertizedDatum::TimestampTz)"
    )]
    TimestampTz(chrono::DateTime<chrono::Utc>),

    #[proptest(strategy = "arb_interval().prop_map(PropertizedDatum::Interval)")]
    Interval(Interval),
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

fn arb_array_dimension() -> BoxedStrategy<ArrayDimension> {
    (1..8_usize)
        .prop_map(|length| ArrayDimension {
            lower_bound: 1,
            length,
        })
        .boxed()
}

#[derive(Debug, PartialEq, Clone)]
struct PropertizedArray(Row, Vec<PropertizedDatum>);

fn arb_propertized_array() -> BoxedStrategy<PropertizedArray> {
    prop::collection::vec(
        arb_array_dimension(),
        1..(repr::adt::array::MAX_ARRAY_DIMENSIONS as usize),
    )
    .prop_flat_map(|dimensions| {
        let n_elts: usize = dimensions.iter().map(|d| d.length).product();
        (
            Just(dimensions),
            prop::collection::vec(any::<PropertizedDatum>(), n_elts),
        )
    })
    .prop_map(|(dimensions, elements)| {
        let element_datums: Vec<Datum<'_>> = elements.iter().map(|pd| pd.into()).collect();
        let mut packer = RowPacker::new();
        packer.push_array(&dimensions, element_datums).unwrap();
        PropertizedArray(packer.finish(), elements)
    })
    .boxed()
}

fn arb_interval() -> BoxedStrategy<Interval> {
    (
        any::<i32>(),
        (-193_273_528_233_599_999_999_000_i128..193_273_528_233_599_999_999_000_i128),
    )
        .prop_map(|(months, duration)| Interval { months, duration })
        .boxed()
}

fn add_arb_duration<T: 'static + Copy + Add<chrono::Duration> + std::fmt::Debug>(
    to: T,
) -> BoxedStrategy<T::Output>
where
    T::Output: std::fmt::Debug,
{
    any::<i64>()
        .prop_map(move |v| to + chrono::Duration::nanoseconds(v))
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
            Time(t) => Datum::from(*t),
            Timestamp(t) => Datum::from(*t),
            TimestampTz(t) => Datum::from(*t),
            Interval(i) => Datum::from(*i),
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
    fn array_packing_unpacks_correctly(array in arb_propertized_array()) {
        let PropertizedArray(row, elts) = array;
        let datums: Vec<Datum<'_>> = elts.iter().map(|e| e.into()).collect();
        let unpacked_datums: Vec<Datum<'_>> = row.unpack_first().unwrap_array().elements().iter().collect();
        assert_eq!(unpacked_datums, datums);
    }

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
