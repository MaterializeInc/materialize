// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Add;

use chrono::TimeZone;
use proptest::prelude::*;
use uuid::Uuid;

use mz_repr::adt::numeric::Numeric as AdtNumeric;
use mz_repr::adt::{array::ArrayDimension, interval::Interval};
use mz_repr::{Datum, Row};

/// A type similar to [`Datum`] that can be proptest-generated.
#[derive(Debug, PartialEq, Clone)]
enum PropertizedDatum {
    Null,
    Bool(bool),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),

    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    TimestampTz(chrono::DateTime<chrono::Utc>),

    Interval(Interval),
    Numeric(AdtNumeric),

    Bytes(Vec<u8>),
    String(String),

    Array(PropertizedArray),
    List(PropertizedList),
    Map(PropertizedDict),

    JsonNull,
    Uuid(Uuid),
    Dummy,
}

fn arb_datum() -> BoxedStrategy<PropertizedDatum> {
    let leaf = prop_oneof![
        Just(PropertizedDatum::Null),
        any::<bool>().prop_map(PropertizedDatum::Bool),
        any::<i16>().prop_map(PropertizedDatum::Int16),
        any::<i32>().prop_map(PropertizedDatum::Int32),
        any::<i64>().prop_map(PropertizedDatum::Int64),
        any::<f32>().prop_map(PropertizedDatum::Float32),
        any::<f64>().prop_map(PropertizedDatum::Float64),
        add_arb_duration(chrono::NaiveDate::from_ymd(2000, 1, 1)).prop_map(PropertizedDatum::Date),
        add_arb_duration(chrono::NaiveTime::from_hms(0, 0, 0)).prop_map(PropertizedDatum::Time),
        add_arb_duration(chrono::NaiveDateTime::from_timestamp(0, 0))
            .prop_map(PropertizedDatum::Timestamp),
        add_arb_duration(chrono::Utc.timestamp(0, 0)).prop_map(PropertizedDatum::TimestampTz),
        arb_interval().prop_map(PropertizedDatum::Interval),
        arb_numeric().prop_map(PropertizedDatum::Numeric),
        prop::collection::vec(any::<u8>(), 1024).prop_map(PropertizedDatum::Bytes),
        ".*".prop_map(PropertizedDatum::String),
        Just(PropertizedDatum::JsonNull),
        Just(PropertizedDatum::Uuid(Uuid::nil())),
        Just(PropertizedDatum::Dummy)
    ];
    leaf.prop_recursive(3, 8, 16, |inner| {
        prop_oneof!(
            arb_array(inner.clone()).prop_map(PropertizedDatum::Array),
            arb_list(inner.clone()).prop_map(PropertizedDatum::List),
            arb_dict(inner).prop_map(PropertizedDatum::Map),
        )
    })
    .boxed()
}

fn arb_array_dimension() -> BoxedStrategy<ArrayDimension> {
    (1..4_usize)
        .prop_map(|length| ArrayDimension {
            lower_bound: 1,
            length,
        })
        .boxed()
}

#[derive(Debug, PartialEq, Clone)]
struct PropertizedArray(Row, Vec<PropertizedDatum>);

fn arb_array(element_strategy: BoxedStrategy<PropertizedDatum>) -> BoxedStrategy<PropertizedArray> {
    prop::collection::vec(
        arb_array_dimension(),
        1..(mz_repr::adt::array::MAX_ARRAY_DIMENSIONS as usize),
    )
    .prop_flat_map(move |dimensions| {
        let n_elts: usize = dimensions.iter().map(|d| d.length).product();
        (
            Just(dimensions),
            prop::collection::vec(element_strategy.clone(), n_elts),
        )
    })
    .prop_map(|(dimensions, elements)| {
        let element_datums: Vec<Datum<'_>> = elements.iter().map(|pd| pd.into()).collect();
        let mut row = Row::default();
        row.packer()
            .push_array(&dimensions, element_datums)
            .unwrap();
        PropertizedArray(row, elements)
    })
    .boxed()
}

#[derive(Debug, PartialEq, Clone)]
struct PropertizedList(Row, Vec<PropertizedDatum>);

fn arb_list(element_strategy: BoxedStrategy<PropertizedDatum>) -> BoxedStrategy<PropertizedList> {
    prop::collection::vec(element_strategy, 1..50)
        .prop_map(|elements| {
            let element_datums: Vec<Datum<'_>> = elements.iter().map(|pd| pd.into()).collect();
            let mut row = Row::default();
            row.packer().push_list(element_datums.iter());
            PropertizedList(row, elements)
        })
        .boxed()
}

#[derive(Debug, PartialEq, Clone)]
struct PropertizedDict(Row, Vec<(String, PropertizedDatum)>);

fn arb_dict(element_strategy: BoxedStrategy<PropertizedDatum>) -> BoxedStrategy<PropertizedDict> {
    prop::collection::vec((".*", element_strategy), 1..50)
        .prop_map(|mut entries| {
            entries.sort_by_key(|(k, _)| k.clone());
            entries.dedup_by_key(|(k, _)| k.clone());
            let mut row = Row::default();
            let entry_iter: Vec<(&str, Datum<'_>)> = entries
                .iter()
                .map(|(k, v)| (k.as_str(), v.into()))
                .collect();
            row.packer().push_dict(entry_iter.into_iter());
            PropertizedDict(row, entries)
        })
        .boxed()
}

fn arb_interval() -> BoxedStrategy<Interval> {
    (
        any::<i32>(),
        any::<i32>(),
        ((((i64::from(i32::MIN) * 60) - 59) * 60) * 1_000_000 - 59_999_999
            ..(((i64::from(i32::MAX) * 60) + 59) * 60) * 1_000_000 + 59_999_999),
    )
        .prop_map(|(months, days, micros)| Interval {
            months,
            days,
            micros,
        })
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

fn arb_numeric() -> BoxedStrategy<AdtNumeric> {
    any::<i128>()
        .prop_map(|v| AdtNumeric::try_from(v).unwrap())
        .boxed()
}

impl<'a> Into<Datum<'a>> for &'a PropertizedDatum {
    fn into(self) -> Datum<'a> {
        use PropertizedDatum::*;
        match self {
            Null => Datum::Null,
            Bool(b) => Datum::from(*b),
            Int16(i) => Datum::from(*i),
            Int32(i) => Datum::from(*i),
            Int64(i) => Datum::from(*i),
            Float32(f) => Datum::from(*f),
            Float64(f) => Datum::from(*f),
            Date(d) => Datum::from(*d),
            Time(t) => Datum::from(*t),
            Timestamp(t) => Datum::from(*t),
            TimestampTz(t) => Datum::from(*t),
            Interval(i) => Datum::from(*i),
            Numeric(s) => Datum::from(*s),
            Bytes(b) => Datum::from(&b[..]),
            String(s) => Datum::from(s.as_str()),
            Array(PropertizedArray(row, _)) => {
                let array = row.unpack_first().unwrap_array();
                Datum::Array(array)
            }
            List(PropertizedList(row, _)) => {
                let list = row.unpack_first().unwrap_list();
                Datum::List(list)
            }
            Map(PropertizedDict(row, _)) => {
                let map = row.unpack_first().unwrap_map();
                Datum::Map(map)
            }
            JsonNull => Datum::JsonNull,
            Uuid(u) => Datum::from(*u),
            Dummy => Datum::Dummy,
        }
    }
}

proptest! {
    #[test]
    fn array_packing_unpacks_correctly(array in arb_array(arb_datum())) {
        let PropertizedArray(row, elts) = array;
        let datums: Vec<Datum<'_>> = elts.iter().map(|e| e.into()).collect();
        let unpacked_datums: Vec<Datum<'_>> = row.unpack_first().unwrap_array().elements().iter().collect();
        assert_eq!(unpacked_datums, datums);
    }

    #[test]
    fn list_packing_unpacks_correctly(array in arb_list(arb_datum())) {
        let PropertizedList(row, elts) = array;
        let datums: Vec<Datum<'_>> = elts.iter().map(|e| e.into()).collect();
        let unpacked_datums: Vec<Datum<'_>> = row.unpack_first().unwrap_list().iter().collect();
        assert_eq!(unpacked_datums, datums);
    }

    #[test]
    fn dict_packing_unpacks_correctly(array in arb_dict(arb_datum())) {
        let PropertizedDict(row, elts) = array;
        let datums: Vec<(&str, Datum<'_>)> = elts.iter().map(|(k, e)| (k.as_str(), e.into())).collect();
        let unpacked_datums: Vec<(&str, Datum<'_>)> = row.unpack_first().unwrap_map().iter().collect();
        assert_eq!(unpacked_datums, datums);
    }

    #[test]
    fn row_packing_roundtrips_single_valued(prop_datums in prop::collection::vec(arb_datum(), 1..100)) {
        let datums: Vec<Datum<'_>> = prop_datums.iter().map(|pd| pd.into()).collect();
        let row = Row::pack(&datums);
        let unpacked = row.unpack();
        assert_eq!(datums, unpacked);
    }
}
