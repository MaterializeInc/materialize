// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::iter;
use std::ops::RangeInclusive;

use chrono::NaiveDate;
use dec::OrderedDecimal;
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use mz_ore::now::NowFn;
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::{self, DecimalLike, Numeric};
use mz_repr::{Datum, Row};
use mz_storage_client::types::sources::{Generator, GeneratorMessageType};

#[derive(Clone, Debug)]
pub struct Tpch {
    pub count_supplier: i64,
    pub count_part: i64,
    pub count_customer: i64,
    pub count_orders: i64,
    pub count_clerk: i64,
}

const SUPPLIER_OUTPUT: usize = 1;
const PART_OUTPUT: usize = 2;
const PARTSUPP_OUTPUT: usize = 3;
const CUSTOMER_OUTPUT: usize = 4;
const ORDERS_OUTPUT: usize = 5;
const LINEITEM_OUTPUT: usize = 6;
const NATION_OUTPUT: usize = 7;
const REGION_OUTPUT: usize = 8;

impl Generator for Tpch {
    fn by_seed(
        &self,
        _: NowFn,
        seed: Option<u64>,
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row)>> {
        let Self {
            count_supplier,
            count_part,
            count_customer,
            count_orders,
            count_clerk,
        } = self.clone();
        let count_nation: i64 = NATIONS.len().try_into().unwrap();
        let count_region: i64 = REGIONS.len().try_into().unwrap();

        let mut rows = (0..count_supplier)
            .map(|i| (SUPPLIER_OUTPUT, i))
            .chain((1..=count_part).map(|i| (PART_OUTPUT, i)))
            .chain((1..=count_customer).map(|i| (CUSTOMER_OUTPUT, i)))
            .chain((1..=count_orders).map(|i| (ORDERS_OUTPUT, i)))
            .chain((0..count_nation).map(|i| (NATION_OUTPUT, i)))
            .chain((0..count_region).map(|i| (REGION_OUTPUT, i)))
            .peekable();

        let mut rng = StdRng::seed_from_u64(seed.unwrap_or_default());
        // Some rows need to generate other rows from their values; hold those
        // here.
        let mut pending = Vec::new();
        let mut cx = numeric::cx_datum();
        let decimal_one = Numeric::lossy_from(1);
        let decimal_neg_one = Numeric::lossy_from(-1);
        // TODO: Use a text generator closer to the spec.
        let text_string_source = Alphanumeric.sample_string(&mut rng, 3 << 20);

        Box::new(iter::from_fn(move || {
            if let Some((output, row)) = pending.pop() {
                // InProgress is safe to use here because REGIONS is the last
                // table, and it doesn't use pending.
                return Some((output, GeneratorMessageType::InProgress, row));
            }
            rows.next().map(|(output, key)| {
                let row = match output {
                    SUPPLIER_OUTPUT => {
                        let nation = rng.gen_range(0..(NATIONS.len() as i64));
                        Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::String(&pad_nine("Supplier", key)),
                            Datum::String(&v_string(&mut rng, 10, 40)), // address
                            Datum::Int64(nation),
                            Datum::String(&phone(&mut rng, nation)),
                            Datum::Numeric(decimal(&mut rng, &mut cx, -999_99, 9_999_99, 100)), // acctbal
                            // TODO: add customer complaints and recommends, see 4.2.3.
                            Datum::String(text_string(&mut rng, &text_string_source, 25, 100)),
                        ])
                    }
                    PART_OUTPUT => {
                        let name: String = PARTNAMES
                            .choose_multiple(&mut rng, 5)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join("  ");
                        let m = rng.gen_range(1..=5);
                        let n = rng.gen_range(1..=5);
                        for _ in 1..=4 {
                            let suppkey = (key
                                + (rng.gen_range(0..=3)
                                    * ((count_supplier / 4) + (key - 1) / count_supplier)))
                                % count_supplier
                                + 1;
                            let row = Row::pack_slice(&[
                                Datum::Int64(key),
                                Datum::Int64(suppkey),
                                Datum::Int32(rng.gen_range(1..=9_999)), // availqty
                                Datum::Numeric(decimal(&mut rng, &mut cx, 1_00, 1_000_00, 100)), // supplycost
                                Datum::String(text_string(&mut rng, &text_string_source, 49, 198)),
                            ]);
                            pending.push((PARTSUPP_OUTPUT, row));
                        }
                        Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::String(&name),
                            Datum::String(&format!("Manufacturer#{m}")),
                            Datum::String(&format!("Brand#{m}{n}")),
                            Datum::String(&syllables(&mut rng, TYPES)),
                            Datum::Int32(rng.gen_range(1..=50)), // size
                            Datum::String(&syllables(&mut rng, CONTAINERS)),
                            Datum::Numeric(partkey_retailprice(key)),
                            Datum::String(text_string(&mut rng, &text_string_source, 49, 198)),
                        ])
                    }
                    CUSTOMER_OUTPUT => {
                        let nation = rng.gen_range(0..(NATIONS.len() as i64));
                        Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::String(&pad_nine("Customer", key)),
                            Datum::String(&v_string(&mut rng, 10, 40)), // address
                            Datum::Int64(nation),
                            Datum::String(&phone(&mut rng, nation)),
                            Datum::Numeric(decimal(&mut rng, &mut cx, -999_99, 9_999_99, 100)), // acctbal
                            Datum::String(SEGMENTS.choose(&mut rng).unwrap()),
                            Datum::String(text_string(&mut rng, &text_string_source, 29, 116)),
                        ])
                    }
                    ORDERS_OUTPUT => {
                        let key = order_key(key);
                        let custkey = loop {
                            let custkey = rng.gen_range(1..=count_customer);
                            if custkey % 3 != 0 {
                                break custkey;
                            }
                        };
                        let orderdate = date(&mut rng, &*START_DATE, 1..=*ORDER_END_DAYS);
                        let mut totalprice = Numeric::lossy_from(0);
                        let mut orderstatus = None;

                        for linenumber in 1..=rng.gen_range(1..=7) {
                            let partkey = rng.gen_range(1..=count_part);
                            let suppkey = (partkey
                                + (rng.gen_range(0..=3)
                                    * ((count_supplier / 4) + (partkey - 1) / count_supplier)))
                                % count_supplier
                                + 1;
                            let quantity = Numeric::from(rng.gen_range(1..=50));
                            let mut extendedprice = quantity;
                            cx.mul(&mut extendedprice, &partkey_retailprice(partkey).0);
                            let mut discount = decimal(&mut rng, &mut cx, 0, 8, 100);
                            let mut tax = decimal(&mut rng, &mut cx, 0, 10, 100);
                            let shipdate = date(&mut rng, &orderdate, 1..=121);
                            let receiptdate = date(&mut rng, &shipdate, 1..=30);
                            let linestatus = if shipdate > *CURRENT_DATE { "O" } else { "F" };
                            let row = Row::pack_slice(&[
                                Datum::Int64(key),
                                Datum::Int64(partkey),
                                Datum::Int64(suppkey),
                                Datum::Int32(linenumber),
                                Datum::Numeric(OrderedDecimal(quantity)),
                                Datum::Numeric(OrderedDecimal(extendedprice)),
                                Datum::Numeric(discount),
                                Datum::Numeric(tax),
                                Datum::String(if receiptdate <= *CURRENT_DATE {
                                    ["R", "A"].choose(&mut rng).unwrap()
                                } else {
                                    "N"
                                }), // returnflag
                                Datum::String(linestatus),
                                Datum::Date(shipdate),
                                Datum::Date(date(&mut rng, &orderdate, 30..=90)), // commitdate
                                Datum::Date(receiptdate),
                                Datum::String(INSTRUCTIONS.choose(&mut rng).unwrap()),
                                Datum::String(MODES.choose(&mut rng).unwrap()),
                                Datum::String(text_string(&mut rng, &text_string_source, 10, 43)),
                            ]);
                            pending.push((LINEITEM_OUTPUT, row));
                            // totalprice += extendedprice * (1.0 + tax) * (1.0 - discount)
                            cx.add(&mut tax.0, &decimal_one);
                            cx.sub(&mut discount.0, &decimal_neg_one);
                            cx.abs(&mut discount.0);
                            cx.mul(&mut extendedprice, &tax.0);
                            cx.mul(&mut extendedprice, &discount.0);
                            cx.add(&mut totalprice, &extendedprice);
                            if let Some(status) = orderstatus {
                                if status != linestatus {
                                    orderstatus = Some("P");
                                }
                            } else {
                                orderstatus = Some(linestatus);
                            }
                        }
                        Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::Int64(custkey),
                            Datum::String(orderstatus.unwrap()),
                            Datum::Numeric(OrderedDecimal(totalprice)),
                            Datum::Date(orderdate),
                            Datum::String(PRIORITIES.choose(&mut rng).unwrap()),
                            Datum::String(&pad_nine("Clerk", rng.gen_range(1..=count_clerk))),
                            Datum::Int32(0), // shippriority
                            Datum::String(text_string(&mut rng, &text_string_source, 19, 78)),
                        ])
                    }
                    NATION_OUTPUT => {
                        let (name, region) = NATIONS[key as usize];
                        Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::String(name),
                            Datum::Int64(region),
                            Datum::String(text_string(&mut rng, &text_string_source, 31, 114)),
                        ])
                    }
                    REGION_OUTPUT => Row::pack_slice(&[
                        Datum::Int64(key),
                        Datum::String(REGIONS[key as usize]),
                        Datum::String(text_string(&mut rng, &text_string_source, 31, 115)),
                    ]),
                    _ => unreachable!("{output}"),
                };
                let typ = if rows.peek().is_some() {
                    GeneratorMessageType::InProgress
                } else {
                    GeneratorMessageType::Finalized
                };
                (output, typ, row)
            })
        }))
    }
}

fn partkey_retailprice(key: i64) -> OrderedDecimal<Numeric> {
    let price = (90000 + ((key / 10) % 20001) + 100 * (key % 1000)) / 100;
    OrderedDecimal(Numeric::from(price))
}

fn pad_nine<S: Display>(prefix: &str, s: S) -> String {
    format!("{}#{s:09}", prefix)
}

pub static START_DATE: Lazy<Date> =
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd(1992, 1, 1)).unwrap());
pub static CURRENT_DATE: Lazy<Date> =
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd(1995, 6, 17)).unwrap());
pub static END_DATE: Lazy<Date> =
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd(1998, 12, 31)).unwrap());
pub static ORDER_END_DAYS: Lazy<i32> = Lazy::new(|| *END_DATE - *START_DATE - 151);

fn text_string<'a, R: Rng + ?Sized>(
    rng: &mut R,
    source: &'a str,
    min: usize,
    max: usize,
) -> &'a str {
    let start = rng.gen_range(0..=(source.len() - max));
    let len = rng.gen_range(min..=max);
    &source[start..(start + len)]
}

fn date<R: Rng + ?Sized>(rng: &mut R, start: &Date, days: RangeInclusive<i32>) -> Date {
    let days = rng.gen_range(days);
    start.checked_add(days).expect("must fit")
}

// See mk_sparse in dbgen's build.c.
fn order_key(mut i: i64) -> i64 {
    const SPARSE_BITS: usize = 2;
    const SPARSE_KEEP: usize = 3;
    let low_bits = i & ((1 << SPARSE_KEEP) - 1);
    i >>= SPARSE_KEEP;
    i <<= SPARSE_BITS;
    // build.c has a `i += seq` here which allows generating multiple data sets
    // in flat files.
    i <<= SPARSE_KEEP;
    i += low_bits;
    i
}

fn syllables<R: Rng + ?Sized>(rng: &mut R, syllables: &[&[&str]]) -> String {
    let mut s = String::new();
    for (i, syllable) in syllables.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(syllable.choose(rng).unwrap());
    }
    s
}

fn decimal<R: Rng + ?Sized>(
    rng: &mut R,
    cx: &mut dec::Context<Numeric>,
    min: i64,
    max: i64,
    div: i64,
) -> OrderedDecimal<Numeric> {
    let n = rng.gen_range(min..=max);
    let mut n = Numeric::lossy_from(n);
    cx.div(&mut n, &Numeric::lossy_from(div));
    OrderedDecimal(n)
}

fn phone<R: Rng + ?Sized>(rng: &mut R, nation: i64) -> String {
    let mut s = String::with_capacity(15);
    s.push_str(&(nation + 10).to_string());
    s.push('-');
    s.push_str(&rng.gen_range(100..=999).to_string());
    s.push('-');
    s.push_str(&rng.gen_range(100..=999).to_string());
    s.push('-');
    s.push_str(&rng.gen_range(1000..=9999).to_string());
    s
}

fn v_string<R: Rng + ?Sized>(rng: &mut R, min: usize, max: usize) -> String {
    const ALPHABET: [char; 64] = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
        'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '1', '2',
        '3', '4', '5', '6', '7', '8', '9', '0', ',', ' ',
    ];
    let take = rng.gen_range(min..=max);
    let mut s = String::with_capacity(take);
    for _ in 0..take {
        s.push(*ALPHABET.choose(rng).unwrap());
    }
    s
}

const INSTRUCTIONS: &[&str] = &[
    "DELIVER IN PERSON",
    "COLLECT COD",
    "NONE",
    "TAKE BACK RETURN",
];

const MODES: &[&str] = &["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"];

const PARTNAMES: &[&str] = &[
    "almond",
    "antique",
    "aquamarine",
    "azure",
    "beige",
    "bisque",
    "black",
    "blanched",
    "blue",
    "blush",
    "brown",
    "burlywood",
    "burnished",
    "chartreuse",
    "chiffon",
    "chocolate",
    "coral",
    "cornflower",
    "cornsilk",
    "cream",
    "cyan",
    "dark",
    "deep",
    "dim",
    "dodger",
    "drab",
    "firebrick",
    "floral",
    "forest",
    "frosted",
    "gainsboro",
    "ghost",
    "goldenrod",
    "green",
    "grey",
    "honeydew",
    "hot",
    "indian",
    "ivory",
    "khaki",
    "lace",
    "lavender",
    "lawn",
    "lemon",
    "light",
    "lime",
    "linen",
    "magenta",
    "maroon",
    "medium",
    "metallic",
    "midnight",
    "mint",
    "misty",
    "moccasin",
    "navajo",
    "navy",
    "olive",
    "orange",
    "orchid",
    "pale",
    "papaya",
    "peach",
    "peru",
    "pink",
    "plum",
    "powder",
    "puff",
    "purple",
    "red",
    "rose",
    "rosy",
    "royal",
    "saddle",
    "salmon",
    "sandy",
    "seashell",
    "sienna",
    "sky",
    "slate",
    "smoke",
    "snow",
    "spring",
    "steel",
    "tan",
    "thistle",
    "tomato",
    "turquoise",
    "violet",
    "wheat",
    "white",
    "yellow",
];

const PRIORITIES: &[&str] = &["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED"];

const TYPES: &[&[&str]] = &[
    &["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"],
    &["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"],
    &["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"],
];

const CONTAINERS: &[&[&str]] = &[
    &["SM", "MED", "JUMBO", "WRAP"],
    &["BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"],
];

const SEGMENTS: &[&str] = &[
    "AUTOMOBILE",
    "BUILDING",
    "FURNITURE",
    "MACHINERY",
    "HOUSEHOLD",
];

const REGIONS: &[&str] = &["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

const NATIONS: &[(&str, i64)] = &[
    ("ALGERIA", 0),
    ("ARGENTINA", 1),
    ("BRAZIL", 1),
    ("CANADA", 1),
    ("EGYPT", 4),
    ("ETHIOPIA", 0),
    ("FRANCE", 3),
    ("GERMANY", 3),
    ("INDIA", 2),
    ("INDONESIA", 2),
    ("IRAN", 4),
    ("IRAQ", 4),
    ("JAPAN", 2),
    ("JORDAN", 4),
    ("KENYA", 0),
    ("MOROCCO", 0),
    ("MOZAMBIQUE", 0),
    ("PERU", 1),
    ("CHINA", 2),
    ("ROMANIA", 3),
    ("SAUDI ARABIA", 4),
    ("VIETNAM", 2),
    ("RUSSIA", 3),
    ("UNITED KINGDOM", 3),
    ("UNITED STATES", 1),
];
