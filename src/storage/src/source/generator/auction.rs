// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::VecDeque, iter};

use rand::prelude::{Rng, SmallRng};
use rand::seq::SliceRandom;
use rand::SeedableRng;

use mz_expr::func::cast_timestamp_tz_to_string;
use mz_ore::now::{to_datetime, NowFn};
use mz_repr::{Datum, RelationDesc, Row, ScalarType};

use crate::types::sources::encoding::DataEncodingInner;
use crate::types::sources::Generator;

pub struct Auction {}

impl Generator for Auction {
    fn data_encoding_inner(&self) -> DataEncodingInner {
        DataEncodingInner::RowCodec(
            RelationDesc::empty()
                .with_column("table", ScalarType::String.nullable(false))
                .with_column(
                    "row_data",
                    ScalarType::List {
                        element_type: Box::new(ScalarType::String),
                        custom_id: None,
                    }
                    .nullable(false),
                ),
        )
    }

    fn views(&self) -> Vec<(&str, RelationDesc)> {
        vec![
            (
                "auctions",
                RelationDesc::empty()
                    .with_column("id", ScalarType::Int64.nullable(false))
                    .with_column("item", ScalarType::String.nullable(false))
                    .with_column("end_time", ScalarType::TimestampTz.nullable(false)),
            ),
            (
                "bids",
                RelationDesc::empty()
                    .with_column("id", ScalarType::Int64.nullable(false))
                    .with_column("auction_id", ScalarType::Int64.nullable(false))
                    .with_column("amount", ScalarType::Int32.nullable(false))
                    .with_column("bid_time", ScalarType::TimestampTz.nullable(false)),
            ),
        ]
    }

    fn by_seed(&self, now: NowFn, seed: Option<u64>) -> Box<dyn Iterator<Item = Vec<Row>>> {
        let mut pending = VecDeque::new();
        let mut rng = SmallRng::seed_from_u64(seed.unwrap_or_default());
        let mut counter = 0;
        Box::new(iter::from_fn(move || {
            {
                if pending.is_empty() {
                    counter += 1;
                    let now = to_datetime(now());
                    let mut auction = Row::with_capacity(2);
                    let mut packer = auction.packer();
                    packer.push(Datum::String("auctions"));
                    packer.push_list(&[
                        Datum::String(&counter.to_string()),               // auction id
                        Datum::String(AUCTIONS.choose(&mut rng).unwrap()), // item
                        Datum::String(&cast_timestamp_tz_to_string(
                            now + chrono::Duration::seconds(10),
                        )), // end time
                    ]);
                    pending.push_back(vec![auction]);
                    const MAX_BIDS: i64 = 10;
                    for i in 0..rng.gen_range(2..MAX_BIDS) {
                        let mut bid = Row::with_capacity(2);
                        let mut packer = bid.packer();
                        packer.push(Datum::String("bids"));
                        packer.push_list(&[
                            Datum::String(&(counter * MAX_BIDS + i).to_string()), // bid id
                            Datum::String(&counter.to_string()),                  // auction id
                            Datum::String(&rng.gen_range(1..100).to_string()),    // amount
                            Datum::String(&cast_timestamp_tz_to_string(
                                now + chrono::Duration::seconds(i),
                            )), // bid time
                        ]);
                        pending.push_back(vec![bid]);
                    }
                }
                pending.pop_front()
            }
        }))
    }
}

const AUCTIONS: &[&str] = &[
    "Signed Memorabilia",
    "City Bar Crawl",
    "Best Pizza in Town",
    "Gift Basket",
    "Custom Art",
];
