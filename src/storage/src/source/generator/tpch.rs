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
use std::time::Duration;

use chrono::NaiveDate;
use dec::{Context as DecimalContext, OrderedDecimal};
use mz_ore::now::NowFn;
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::{self, DecimalLike, Numeric};
use mz_repr::{Datum, Row};
use mz_storage_client::types::sources::{Generator, GeneratorMessageType};
use once_cell::sync::Lazy;
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

#[derive(Clone, Debug)]
pub struct Tpch {
    pub count_supplier: i64,
    pub count_part: i64,
    pub count_customer: i64,
    pub count_orders: i64,
    pub count_clerk: i64,
    pub tick: Duration,
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
    ) -> Box<dyn Iterator<Item = (usize, GeneratorMessageType, Row, i64)>> {
        let mut rng = StdRng::seed_from_u64(seed.unwrap_or_default());
        let mut ctx = Context {
            tpch: self.clone(),
            decimal_one: Numeric::from(1),
            decimal_neg_one: Numeric::from(-1),
            cx: numeric::cx_datum(),
            // TODO: Use a text generator closer to the spec.
            text_string_source: Alphanumeric.sample_string(&mut rng, 3 << 20),
        };

        let count_nation: i64 = NATIONS.len().try_into().unwrap();
        let count_region: i64 = REGIONS.len().try_into().unwrap();

        let mut rows = (0..ctx.tpch.count_supplier)
            .map(|i| (SUPPLIER_OUTPUT, i))
            .chain((1..=ctx.tpch.count_part).map(|i| (PART_OUTPUT, i)))
            .chain((1..=ctx.tpch.count_customer).map(|i| (CUSTOMER_OUTPUT, i)))
            .chain((1..=ctx.tpch.count_orders).map(|i| (ORDERS_OUTPUT, i)))
            .chain((0..count_nation).map(|i| (NATION_OUTPUT, i)))
            .chain((0..count_region).map(|i| (REGION_OUTPUT, i)))
            .peekable();

        // Some rows need to generate other rows from their values; hold those
        // here.
        let mut pending = Vec::new();

        // All orders and their lineitems, so they can be retracted during
        // streaming.
        let mut active_orders = Vec::new();

        Box::new(iter::from_fn(move || {
            if let Some(pending) = pending.pop() {
                return Some(pending);
            }
            rows.next()
                .map(|(output, key)| {
                    let key_usize = usize::try_from(key).expect("key known to be non-negative");
                    let row = match output {
                        SUPPLIER_OUTPUT => {
                            let nation = rng.gen_range(0..count_nation);
                            Row::pack_slice(&[
                                Datum::Int64(key),
                                Datum::String(&pad_nine("Supplier", key)),
                                Datum::String(&v_string(&mut rng, 10, 40)), // address
                                Datum::Int64(nation),
                                Datum::String(&phone(&mut rng, nation)),
                                Datum::Numeric(decimal(
                                    &mut rng,
                                    &mut ctx.cx,
                                    -999_99,
                                    9_999_99,
                                    100,
                                )), // acctbal
                                // TODO: add customer complaints and recommends, see 4.2.3.
                                Datum::String(text_string(
                                    &mut rng,
                                    &ctx.text_string_source,
                                    25,
                                    100,
                                )),
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
                                        * ((ctx.tpch.count_supplier / 4)
                                            + (key - 1) / ctx.tpch.count_supplier)))
                                    % ctx.tpch.count_supplier
                                    + 1;
                                let row = Row::pack_slice(&[
                                    Datum::Int64(key),
                                    Datum::Int64(suppkey),
                                    Datum::Int32(rng.gen_range(1..=9_999)), // availqty
                                    Datum::Numeric(decimal(
                                        &mut rng,
                                        &mut ctx.cx,
                                        1_00,
                                        1_000_00,
                                        100,
                                    )), // supplycost
                                    Datum::String(text_string(
                                        &mut rng,
                                        &ctx.text_string_source,
                                        49,
                                        198,
                                    )),
                                ]);
                                pending.push((
                                    PARTSUPP_OUTPUT,
                                    GeneratorMessageType::InProgress,
                                    row,
                                    1,
                                ));
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
                                Datum::String(text_string(
                                    &mut rng,
                                    &ctx.text_string_source,
                                    49,
                                    198,
                                )),
                            ])
                        }
                        CUSTOMER_OUTPUT => {
                            let nation = rng.gen_range(0..count_nation);
                            Row::pack_slice(&[
                                Datum::Int64(key),
                                Datum::String(&pad_nine("Customer", key)),
                                Datum::String(&v_string(&mut rng, 10, 40)), // address
                                Datum::Int64(nation),
                                Datum::String(&phone(&mut rng, nation)),
                                Datum::Numeric(decimal(
                                    &mut rng,
                                    &mut ctx.cx,
                                    -999_99,
                                    9_999_99,
                                    100,
                                )), // acctbal
                                Datum::String(SEGMENTS.choose(&mut rng).unwrap()),
                                Datum::String(text_string(
                                    &mut rng,
                                    &ctx.text_string_source,
                                    29,
                                    116,
                                )),
                            ])
                        }
                        ORDERS_OUTPUT => {
                            let seed = rng.gen();
                            let (order, lineitems) = ctx.order_row(seed, key);
                            for row in lineitems {
                                pending.push((
                                    LINEITEM_OUTPUT,
                                    GeneratorMessageType::InProgress,
                                    row,
                                    1,
                                ));
                            }
                            if !ctx.tpch.tick.is_zero() {
                                active_orders.push((key, seed));
                            }
                            order
                        }
                        NATION_OUTPUT => {
                            let (name, region) = NATIONS[key_usize];
                            Row::pack_slice(&[
                                Datum::Int64(key),
                                Datum::String(name),
                                Datum::Int64(region),
                                Datum::String(text_string(
                                    &mut rng,
                                    &ctx.text_string_source,
                                    31,
                                    114,
                                )),
                            ])
                        }
                        REGION_OUTPUT => Row::pack_slice(&[
                            Datum::Int64(key),
                            Datum::String(REGIONS[key_usize]),
                            Datum::String(text_string(&mut rng, &ctx.text_string_source, 31, 115)),
                        ]),
                        _ => unreachable!("{output}"),
                    };
                    let typ = if rows.peek().is_some() {
                        GeneratorMessageType::InProgress
                    } else {
                        GeneratorMessageType::Finalized
                    };
                    (output, typ, row, 1)
                })
                .or_else(|| {
                    if ctx.tpch.tick.is_zero() {
                        return None;
                    }
                    let idx = rng.gen_range(0..active_orders.len());
                    let (key, old_seed) = active_orders.swap_remove(idx);
                    let (old_order, old_lineitems) = ctx.order_row(old_seed, key);
                    // Fill pending with old lineitem retractions, new lineitem
                    // additions, and finally the new order. Return the old
                    // order to start the batch.
                    for row in old_lineitems {
                        pending.push((LINEITEM_OUTPUT, GeneratorMessageType::InProgress, row, -1));
                    }
                    let new_seed = rng.gen();
                    let (new_order, new_lineitems) = ctx.order_row(new_seed, key);
                    for row in new_lineitems {
                        pending.push((LINEITEM_OUTPUT, GeneratorMessageType::InProgress, row, 1));
                    }
                    pending.push((ORDERS_OUTPUT, GeneratorMessageType::Finalized, new_order, 1));
                    active_orders.push((key, new_seed));

                    Some((
                        ORDERS_OUTPUT,
                        GeneratorMessageType::InProgress,
                        old_order,
                        -1,
                    ))
                })
        }))
    }
}

struct Context {
    tpch: Tpch,
    decimal_one: Numeric,
    decimal_neg_one: Numeric,
    cx: DecimalContext<Numeric>,
    text_string_source: String,
}

impl Context {
    /// Generate a row based on its key and seed. Used for order retraction
    /// without having to hold onto the full row for the order and its
    /// lineitems.
    fn order_row(&mut self, seed: u64, key: i64) -> (Row, Vec<Row>) {
        let mut rng = StdRng::seed_from_u64(seed);
        let key = order_key(key);
        let custkey = loop {
            let custkey = rng.gen_range(1..=self.tpch.count_customer);
            if custkey % 3 != 0 {
                break custkey;
            }
        };
        let orderdate = date(&mut rng, &*START_DATE, 1..=*ORDER_END_DAYS);
        let mut totalprice = Numeric::lossy_from(0);
        let mut orderstatus = None;
        let lineitem_count = rng.gen_range(1..=7);
        let mut lineitems = Vec::with_capacity(lineitem_count);

        for linenumber in 1..=lineitem_count {
            let partkey = rng.gen_range(1..=self.tpch.count_part);
            let suppkey = (partkey
                + (rng.gen_range(0..=3)
                    * ((self.tpch.count_supplier / 4) + (partkey - 1) / self.tpch.count_supplier)))
                % self.tpch.count_supplier
                + 1;
            let quantity = Numeric::from(rng.gen_range(1..=50));
            let mut extendedprice = quantity;
            self.cx
                .mul(&mut extendedprice, &partkey_retailprice(partkey).0);
            let mut discount = decimal(&mut rng, &mut self.cx, 0, 8, 100);
            let mut tax = decimal(&mut rng, &mut self.cx, 0, 10, 100);
            let shipdate = date(&mut rng, &orderdate, 1..=121);
            let receiptdate = date(&mut rng, &shipdate, 1..=30);
            let linestatus = if shipdate > *CURRENT_DATE { "O" } else { "F" };
            let row = Row::pack_slice(&[
                Datum::Int64(key),
                Datum::Int64(partkey),
                Datum::Int64(suppkey),
                Datum::Int32(linenumber.try_into().expect("must fit")),
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
                Datum::String(text_string(&mut rng, &self.text_string_source, 10, 43)),
            ]);
            // totalprice += extendedprice * (1.0 + tax) * (1.0 - discount)
            self.cx.add(&mut tax.0, &self.decimal_one);
            self.cx.sub(&mut discount.0, &self.decimal_neg_one);
            self.cx.abs(&mut discount.0);
            self.cx.mul(&mut extendedprice, &tax.0);
            self.cx.mul(&mut extendedprice, &discount.0);
            self.cx.add(&mut totalprice, &extendedprice);
            if let Some(status) = orderstatus {
                if status != linestatus {
                    orderstatus = Some("P");
                }
            } else {
                orderstatus = Some(linestatus);
            }
            lineitems.push(row);
        }

        let order = Row::pack_slice(&[
            Datum::Int64(key),
            Datum::Int64(custkey),
            Datum::String(orderstatus.unwrap()),
            Datum::Numeric(OrderedDecimal(totalprice)),
            Datum::Date(orderdate),
            Datum::String(PRIORITIES.choose(&mut rng).unwrap()),
            Datum::String(&pad_nine("Clerk", rng.gen_range(1..=self.tpch.count_clerk))),
            Datum::Int32(0), // shippriority
            Datum::String(text_string(&mut rng, &self.text_string_source, 19, 78)),
        ]);

        (order, lineitems)
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
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd_opt(1992, 1, 1).unwrap()).unwrap());
pub static CURRENT_DATE: Lazy<Date> =
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd_opt(1995, 6, 17).unwrap()).unwrap());
pub static END_DATE: Lazy<Date> =
    Lazy::new(|| Date::try_from(NaiveDate::from_ymd_opt(1998, 12, 31).unwrap()).unwrap());
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
