// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to load generator sources

use std::sync::LazyLock;
use std::time::Duration;

use mz_ore::now::NowFn;
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::numeric::NumericMaxScale;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationDesc, Row, ScalarType};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::AlterCompatible;
use crate::sources::AlterError;
use crate::sources::{MzOffset, SourceConnection};

use super::SourceExportDetails;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.load_generator.rs"
));

pub const LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT: &str = "offset";

/// Data and progress events of the native stream.
#[derive(Debug)]
pub enum Event<F: IntoIterator, D> {
    /// Indicates that timestamps have advanced to frontier F
    Progress(F),
    /// Indicates that event D happened at time T
    Message(F::Item, D),
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LoadGeneratorSourceConnection {
    pub load_generator: LoadGenerator,
    pub tick_micros: Option<u64>,
    pub as_of: u64,
    pub up_to: u64,
}

pub static LOAD_GEN_PROGRESS_DESC: LazyLock<RelationDesc> = LazyLock::new(|| {
    RelationDesc::builder()
        .with_column("offset", ScalarType::UInt64.nullable(true))
        .finish()
});

impl SourceConnection for LoadGeneratorSourceConnection {
    fn name(&self) -> &'static str {
        "load-generator"
    }

    fn external_reference(&self) -> Option<&str> {
        None
    }

    fn default_key_desc(&self) -> RelationDesc {
        match &self.load_generator {
            LoadGenerator::KeyValue(_) => {
                // `"key"` is overridden by the key_envelope in planning.
                RelationDesc::builder()
                    .with_column("key", ScalarType::UInt64.nullable(false))
                    .finish()
            }
            _ => RelationDesc::empty(),
        }
    }

    fn default_value_desc(&self) -> RelationDesc {
        match &self.load_generator {
            LoadGenerator::Auction => RelationDesc::empty(),
            LoadGenerator::Clock => RelationDesc::builder()
                .with_column(
                    "time",
                    ScalarType::TimestampTz { precision: None }.nullable(false),
                )
                .finish(),
            LoadGenerator::Datums => {
                let mut desc =
                    RelationDesc::builder().with_column("rowid", ScalarType::Int64.nullable(false));
                let typs = ScalarType::enumerate();
                let mut names = BTreeSet::new();
                for typ in typs {
                    // Cut out variant information from the debug print.
                    let mut name = format!("_{:?}", typ)
                        .split(' ')
                        .next()
                        .unwrap()
                        .to_lowercase();
                    // Incase we ever have multiple variants of the same type, create
                    // unique names for them.
                    while names.contains(&name) {
                        name.push('_');
                    }
                    names.insert(name.clone());
                    desc = desc.with_column(name, typ.clone().nullable(true));
                }
                desc.finish()
            }
            LoadGenerator::Counter { .. } => RelationDesc::builder()
                .with_column("counter", ScalarType::Int64.nullable(false))
                .finish(),
            LoadGenerator::Marketing => RelationDesc::empty(),
            LoadGenerator::Tpch { .. } => RelationDesc::empty(),
            LoadGenerator::KeyValue(KeyValueLoadGenerator { include_offset, .. }) => {
                let mut desc = RelationDesc::builder()
                    .with_column("partition", ScalarType::UInt64.nullable(false))
                    .with_column("value", ScalarType::Bytes.nullable(false));

                if let Some(offset_name) = include_offset.as_deref() {
                    desc = desc.with_column(offset_name, ScalarType::UInt64.nullable(false));
                }
                desc.finish()
            }
        }
    }

    fn timestamp_desc(&self) -> RelationDesc {
        LOAD_GEN_PROGRESS_DESC.clone()
    }

    fn connection_id(&self) -> Option<CatalogItemId> {
        None
    }

    // Some load-gen types output to their primary collection while
    // others do not.
    fn primary_export_details(&self) -> SourceExportDetails {
        match &self.load_generator {
            LoadGenerator::Auction => SourceExportDetails::None,
            LoadGenerator::Clock => {
                SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                    output: LoadGeneratorOutput::Default,
                })
            }
            LoadGenerator::Datums => {
                SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                    output: LoadGeneratorOutput::Default,
                })
            }
            LoadGenerator::Counter { .. } => {
                SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                    output: LoadGeneratorOutput::Default,
                })
            }
            LoadGenerator::Marketing => SourceExportDetails::None,
            LoadGenerator::Tpch { .. } => SourceExportDetails::None,
            LoadGenerator::KeyValue(_) => {
                SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                    output: LoadGeneratorOutput::Default,
                })
            }
        }
    }

    fn supports_read_only(&self) -> bool {
        true
    }

    fn prefers_single_replica(&self) -> bool {
        false
    }
}

impl crate::AlterCompatible for LoadGeneratorSourceConnection {}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LoadGenerator {
    Auction,
    Clock,
    Counter {
        /// How many values will be emitted
        /// before old ones are retracted, or `None` for
        /// an append-only collection.
        max_cardinality: Option<u64>,
    },
    Datums,
    Marketing,
    Tpch {
        count_supplier: i64,
        count_part: i64,
        count_customer: i64,
        count_orders: i64,
        count_clerk: i64,
    },
    KeyValue(KeyValueLoadGenerator),
}

pub const LOAD_GENERATOR_DATABASE_NAME: &str = "mz_load_generators";

impl LoadGenerator {
    /// Must be kept in-sync with the same mapping on the `LoadGenerator` enum defined in
    /// src/sql-parser/src/ast/defs/ddl.rs.
    pub fn schema_name(&self) -> &'static str {
        match self {
            LoadGenerator::Counter { .. } => "counter",
            LoadGenerator::Clock => "clock",
            LoadGenerator::Marketing => "marketing",
            LoadGenerator::Auction => "auction",
            LoadGenerator::Datums => "datums",
            LoadGenerator::Tpch { .. } => "tpch",
            LoadGenerator::KeyValue { .. } => "key_value",
        }
    }

    /// Returns the list of table names and their column types that this generator generates
    pub fn views(&self) -> Vec<(&str, RelationDesc, LoadGeneratorOutput)> {
        match self {
            LoadGenerator::Auction => vec![
                (
                    "organizations",
                    RelationDesc::builder()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("name", ScalarType::String.nullable(false))
                        .with_key(vec![0])
                        .finish(),
                    LoadGeneratorOutput::Auction(AuctionView::Organizations),
                ),
                (
                    "users",
                    RelationDesc::builder()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("org_id", ScalarType::Int64.nullable(false))
                        .with_column("name", ScalarType::String.nullable(false))
                        .with_key(vec![0])
                        .finish(),
                    LoadGeneratorOutput::Auction(AuctionView::Users),
                ),
                (
                    "accounts",
                    RelationDesc::builder()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("org_id", ScalarType::Int64.nullable(false))
                        .with_column("balance", ScalarType::Int64.nullable(false))
                        .with_key(vec![0])
                        .finish(),
                    LoadGeneratorOutput::Auction(AuctionView::Accounts),
                ),
                (
                    "auctions",
                    RelationDesc::builder()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("seller", ScalarType::Int64.nullable(false))
                        .with_column("item", ScalarType::String.nullable(false))
                        .with_column(
                            "end_time",
                            ScalarType::TimestampTz { precision: None }.nullable(false),
                        )
                        .with_key(vec![0])
                        .finish(),
                    LoadGeneratorOutput::Auction(AuctionView::Auctions),
                ),
                (
                    "bids",
                    RelationDesc::builder()
                        .with_column("id", ScalarType::Int64.nullable(false))
                        .with_column("buyer", ScalarType::Int64.nullable(false))
                        .with_column("auction_id", ScalarType::Int64.nullable(false))
                        .with_column("amount", ScalarType::Int32.nullable(false))
                        .with_column(
                            "bid_time",
                            ScalarType::TimestampTz { precision: None }.nullable(false),
                        )
                        .with_key(vec![0])
                        .finish(),
                    LoadGeneratorOutput::Auction(AuctionView::Bids),
                ),
            ],
            LoadGenerator::Clock => vec![],
            LoadGenerator::Counter { max_cardinality: _ } => vec![],
            LoadGenerator::Marketing => {
                vec![
                    (
                        "customers",
                        RelationDesc::builder()
                            .with_column("id", ScalarType::Int64.nullable(false))
                            .with_column("email", ScalarType::String.nullable(false))
                            .with_column("income", ScalarType::Int64.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::Customers),
                    ),
                    (
                        "impressions",
                        RelationDesc::builder()
                            .with_column("id", ScalarType::Int64.nullable(false))
                            .with_column("customer_id", ScalarType::Int64.nullable(false))
                            .with_column("campaign_id", ScalarType::Int64.nullable(false))
                            .with_column(
                                "impression_time",
                                ScalarType::TimestampTz { precision: None }.nullable(false),
                            )
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::Impressions),
                    ),
                    (
                        "clicks",
                        RelationDesc::builder()
                            .with_column("impression_id", ScalarType::Int64.nullable(false))
                            .with_column(
                                "click_time",
                                ScalarType::TimestampTz { precision: None }.nullable(false),
                            )
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::Clicks),
                    ),
                    (
                        "leads",
                        RelationDesc::builder()
                            .with_column("id", ScalarType::Int64.nullable(false))
                            .with_column("customer_id", ScalarType::Int64.nullable(false))
                            .with_column(
                                "created_at",
                                ScalarType::TimestampTz { precision: None }.nullable(false),
                            )
                            .with_column(
                                "converted_at",
                                ScalarType::TimestampTz { precision: None }.nullable(true),
                            )
                            .with_column("conversion_amount", ScalarType::Int64.nullable(true))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::Leads),
                    ),
                    (
                        "coupons",
                        RelationDesc::builder()
                            .with_column("id", ScalarType::Int64.nullable(false))
                            .with_column("lead_id", ScalarType::Int64.nullable(false))
                            .with_column(
                                "created_at",
                                ScalarType::TimestampTz { precision: None }.nullable(false),
                            )
                            .with_column("amount", ScalarType::Int64.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::Coupons),
                    ),
                    (
                        "conversion_predictions",
                        RelationDesc::builder()
                            .with_column("lead_id", ScalarType::Int64.nullable(false))
                            .with_column("experiment_bucket", ScalarType::String.nullable(false))
                            .with_column(
                                "predicted_at",
                                ScalarType::TimestampTz { precision: None }.nullable(false),
                            )
                            .with_column("score", ScalarType::Float64.nullable(false))
                            .finish(),
                        LoadGeneratorOutput::Marketing(MarketingView::ConversionPredictions),
                    ),
                ]
            }
            LoadGenerator::Datums => vec![],
            LoadGenerator::Tpch { .. } => {
                let identifier = ScalarType::Int64.nullable(false);
                let decimal = ScalarType::Numeric {
                    max_scale: Some(NumericMaxScale::try_from(2i64).unwrap()),
                }
                .nullable(false);
                vec![
                    (
                        "supplier",
                        RelationDesc::builder()
                            .with_column("s_suppkey", identifier.clone())
                            .with_column("s_name", ScalarType::String.nullable(false))
                            .with_column("s_address", ScalarType::String.nullable(false))
                            .with_column("s_nationkey", identifier.clone())
                            .with_column("s_phone", ScalarType::String.nullable(false))
                            .with_column("s_acctbal", decimal.clone())
                            .with_column("s_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Supplier),
                    ),
                    (
                        "part",
                        RelationDesc::builder()
                            .with_column("p_partkey", identifier.clone())
                            .with_column("p_name", ScalarType::String.nullable(false))
                            .with_column("p_mfgr", ScalarType::String.nullable(false))
                            .with_column("p_brand", ScalarType::String.nullable(false))
                            .with_column("p_type", ScalarType::String.nullable(false))
                            .with_column("p_size", ScalarType::Int32.nullable(false))
                            .with_column("p_container", ScalarType::String.nullable(false))
                            .with_column("p_retailprice", decimal.clone())
                            .with_column("p_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Part),
                    ),
                    (
                        "partsupp",
                        RelationDesc::builder()
                            .with_column("ps_partkey", identifier.clone())
                            .with_column("ps_suppkey", identifier.clone())
                            .with_column("ps_availqty", ScalarType::Int32.nullable(false))
                            .with_column("ps_supplycost", decimal.clone())
                            .with_column("ps_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0, 1])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Partsupp),
                    ),
                    (
                        "customer",
                        RelationDesc::builder()
                            .with_column("c_custkey", identifier.clone())
                            .with_column("c_name", ScalarType::String.nullable(false))
                            .with_column("c_address", ScalarType::String.nullable(false))
                            .with_column("c_nationkey", identifier.clone())
                            .with_column("c_phone", ScalarType::String.nullable(false))
                            .with_column("c_acctbal", decimal.clone())
                            .with_column("c_mktsegment", ScalarType::String.nullable(false))
                            .with_column("c_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Customer),
                    ),
                    (
                        "orders",
                        RelationDesc::builder()
                            .with_column("o_orderkey", identifier.clone())
                            .with_column("o_custkey", identifier.clone())
                            .with_column("o_orderstatus", ScalarType::String.nullable(false))
                            .with_column("o_totalprice", decimal.clone())
                            .with_column("o_orderdate", ScalarType::Date.nullable(false))
                            .with_column("o_orderpriority", ScalarType::String.nullable(false))
                            .with_column("o_clerk", ScalarType::String.nullable(false))
                            .with_column("o_shippriority", ScalarType::Int32.nullable(false))
                            .with_column("o_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Orders),
                    ),
                    (
                        "lineitem",
                        RelationDesc::builder()
                            .with_column("l_orderkey", identifier.clone())
                            .with_column("l_partkey", identifier.clone())
                            .with_column("l_suppkey", identifier.clone())
                            .with_column("l_linenumber", ScalarType::Int32.nullable(false))
                            .with_column("l_quantity", decimal.clone())
                            .with_column("l_extendedprice", decimal.clone())
                            .with_column("l_discount", decimal.clone())
                            .with_column("l_tax", decimal)
                            .with_column("l_returnflag", ScalarType::String.nullable(false))
                            .with_column("l_linestatus", ScalarType::String.nullable(false))
                            .with_column("l_shipdate", ScalarType::Date.nullable(false))
                            .with_column("l_commitdate", ScalarType::Date.nullable(false))
                            .with_column("l_receiptdate", ScalarType::Date.nullable(false))
                            .with_column("l_shipinstruct", ScalarType::String.nullable(false))
                            .with_column("l_shipmode", ScalarType::String.nullable(false))
                            .with_column("l_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0, 3])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Lineitem),
                    ),
                    (
                        "nation",
                        RelationDesc::builder()
                            .with_column("n_nationkey", identifier.clone())
                            .with_column("n_name", ScalarType::String.nullable(false))
                            .with_column("n_regionkey", identifier.clone())
                            .with_column("n_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Nation),
                    ),
                    (
                        "region",
                        RelationDesc::builder()
                            .with_column("r_regionkey", identifier)
                            .with_column("r_name", ScalarType::String.nullable(false))
                            .with_column("r_comment", ScalarType::String.nullable(false))
                            .with_key(vec![0])
                            .finish(),
                        LoadGeneratorOutput::Tpch(TpchView::Region),
                    ),
                ]
            }
            LoadGenerator::KeyValue(_) => vec![],
        }
    }

    pub fn is_monotonic(&self) -> bool {
        match self {
            LoadGenerator::Auction => true,
            LoadGenerator::Clock => false,
            LoadGenerator::Counter {
                max_cardinality: None,
            } => true,
            LoadGenerator::Counter { .. } => false,
            LoadGenerator::Marketing => false,
            LoadGenerator::Datums => true,
            LoadGenerator::Tpch { .. } => false,
            LoadGenerator::KeyValue(_) => true,
        }
    }
}

// Used to identify a view of a load-generator source
// such that the source dataflow can output data to the correct
// data output for a source-export using this view
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary, PartialOrd, Ord)]
pub enum LoadGeneratorOutput {
    // Used for outputting to the primary source output
    Default,
    Auction(AuctionView),
    Marketing(MarketingView),
    Tpch(TpchView),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary, PartialOrd, Ord)]
pub enum AuctionView {
    Organizations,
    Users,
    Accounts,
    Auctions,
    Bids,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary, PartialOrd, Ord)]
pub enum MarketingView {
    Customers,
    Impressions,
    Clicks,
    Leads,
    Coupons,
    ConversionPredictions,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary, PartialOrd, Ord)]
pub enum TpchView {
    Supplier,
    Part,
    Partsupp,
    Customer,
    Orders,
    Lineitem,
    Nation,
    Region,
}

// TODO(roshan): Remove these str mappings once the migration
// `ast_rewrite_create_load_gen_subsource_details` is removed,
// since these are only needed for the migration but should not otherwise be used.
impl From<&str> for AuctionView {
    fn from(s: &str) -> Self {
        match s {
            "organizations" => AuctionView::Organizations,
            "users" => AuctionView::Users,
            "accounts" => AuctionView::Accounts,
            "auctions" => AuctionView::Auctions,
            "bids" => AuctionView::Bids,
            _ => panic!("unexpected load generator output name: {}", s),
        }
    }
}

impl From<&str> for MarketingView {
    fn from(s: &str) -> Self {
        match s {
            "customers" => MarketingView::Customers,
            "impressions" => MarketingView::Impressions,
            "clicks" => MarketingView::Clicks,
            "leads" => MarketingView::Leads,
            "coupons" => MarketingView::Coupons,
            "conversion_predictions" => MarketingView::ConversionPredictions,
            _ => panic!("unexpected load generator output name: {}", s),
        }
    }
}

impl From<&str> for TpchView {
    fn from(s: &str) -> Self {
        match s {
            "supplier" => TpchView::Supplier,
            "part" => TpchView::Part,
            "partsupp" => TpchView::Partsupp,
            "customer" => TpchView::Customer,
            "orders" => TpchView::Orders,
            "lineitem" => TpchView::Lineitem,
            "nation" => TpchView::Nation,
            "region" => TpchView::Region,
            _ => panic!("unexpected load generator output name: {}", s),
        }
    }
}

impl RustType<ProtoLoadGeneratorAuctionOutput> for AuctionView {
    fn into_proto(&self) -> ProtoLoadGeneratorAuctionOutput {
        match self {
            AuctionView::Organizations => ProtoLoadGeneratorAuctionOutput::Organizations,
            AuctionView::Users => ProtoLoadGeneratorAuctionOutput::Users,
            AuctionView::Accounts => ProtoLoadGeneratorAuctionOutput::Accounts,
            AuctionView::Auctions => ProtoLoadGeneratorAuctionOutput::Auctions,
            AuctionView::Bids => ProtoLoadGeneratorAuctionOutput::Bids,
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorAuctionOutput) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            ProtoLoadGeneratorAuctionOutput::Organizations => AuctionView::Organizations,
            ProtoLoadGeneratorAuctionOutput::Users => AuctionView::Users,
            ProtoLoadGeneratorAuctionOutput::Accounts => AuctionView::Accounts,
            ProtoLoadGeneratorAuctionOutput::Auctions => AuctionView::Auctions,
            ProtoLoadGeneratorAuctionOutput::Bids => AuctionView::Bids,
        })
    }
}

impl RustType<ProtoLoadGeneratorMarketingOutput> for MarketingView {
    fn into_proto(&self) -> ProtoLoadGeneratorMarketingOutput {
        match self {
            MarketingView::Customers => ProtoLoadGeneratorMarketingOutput::Customers,
            MarketingView::Impressions => ProtoLoadGeneratorMarketingOutput::Impressions,
            MarketingView::Clicks => ProtoLoadGeneratorMarketingOutput::Clicks,
            MarketingView::Leads => ProtoLoadGeneratorMarketingOutput::Leads,
            MarketingView::Coupons => ProtoLoadGeneratorMarketingOutput::Coupons,
            MarketingView::ConversionPredictions => {
                ProtoLoadGeneratorMarketingOutput::ConversionPredictions
            }
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorMarketingOutput) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            ProtoLoadGeneratorMarketingOutput::Customers => MarketingView::Customers,
            ProtoLoadGeneratorMarketingOutput::Impressions => MarketingView::Impressions,
            ProtoLoadGeneratorMarketingOutput::Clicks => MarketingView::Clicks,
            ProtoLoadGeneratorMarketingOutput::Leads => MarketingView::Leads,
            ProtoLoadGeneratorMarketingOutput::Coupons => MarketingView::Coupons,
            ProtoLoadGeneratorMarketingOutput::ConversionPredictions => {
                MarketingView::ConversionPredictions
            }
        })
    }
}

impl RustType<ProtoLoadGeneratorTpchOutput> for TpchView {
    fn into_proto(&self) -> ProtoLoadGeneratorTpchOutput {
        match self {
            TpchView::Supplier => ProtoLoadGeneratorTpchOutput::Supplier,
            TpchView::Part => ProtoLoadGeneratorTpchOutput::Part,
            TpchView::Partsupp => ProtoLoadGeneratorTpchOutput::Partsupp,
            TpchView::Customer => ProtoLoadGeneratorTpchOutput::Customer,
            TpchView::Orders => ProtoLoadGeneratorTpchOutput::Orders,
            TpchView::Lineitem => ProtoLoadGeneratorTpchOutput::Lineitem,
            TpchView::Nation => ProtoLoadGeneratorTpchOutput::Nation,
            TpchView::Region => ProtoLoadGeneratorTpchOutput::Region,
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorTpchOutput) -> Result<Self, TryFromProtoError> {
        Ok(match proto {
            ProtoLoadGeneratorTpchOutput::Supplier => TpchView::Supplier,
            ProtoLoadGeneratorTpchOutput::Part => TpchView::Part,
            ProtoLoadGeneratorTpchOutput::Partsupp => TpchView::Partsupp,
            ProtoLoadGeneratorTpchOutput::Customer => TpchView::Customer,
            ProtoLoadGeneratorTpchOutput::Orders => TpchView::Orders,
            ProtoLoadGeneratorTpchOutput::Lineitem => TpchView::Lineitem,
            ProtoLoadGeneratorTpchOutput::Nation => TpchView::Nation,
            ProtoLoadGeneratorTpchOutput::Region => TpchView::Region,
        })
    }
}

impl RustType<ProtoLoadGeneratorOutput> for LoadGeneratorOutput {
    fn into_proto(&self) -> ProtoLoadGeneratorOutput {
        use proto_load_generator_output::Kind;
        let kind = match self {
            LoadGeneratorOutput::Default => Kind::Default(()),
            LoadGeneratorOutput::Auction(view) => Kind::Auction(view.into_proto().into()),
            LoadGeneratorOutput::Marketing(view) => Kind::Marketing(view.into_proto().into()),
            LoadGeneratorOutput::Tpch(view) => Kind::Tpch(view.into_proto().into()),
        };
        ProtoLoadGeneratorOutput { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoLoadGeneratorOutput) -> Result<Self, TryFromProtoError> {
        use proto_load_generator_output::Kind;
        Ok(match proto.kind {
            Some(Kind::Default(())) => LoadGeneratorOutput::Default,
            Some(Kind::Auction(view)) => LoadGeneratorOutput::Auction(
                ProtoLoadGeneratorAuctionOutput::try_from(view)
                    .map_err(|_| {
                        TryFromProtoError::unknown_enum_variant("ProtoLoadGeneratorAuctionOutput")
                    })?
                    .into_rust()?,
            ),
            Some(Kind::Marketing(view)) => LoadGeneratorOutput::Marketing(
                ProtoLoadGeneratorMarketingOutput::try_from(view)
                    .map_err(|_| {
                        TryFromProtoError::unknown_enum_variant("ProtoLoadGeneratorMarketingOutput")
                    })?
                    .into_rust()?,
            ),
            Some(Kind::Tpch(view)) => LoadGeneratorOutput::Tpch(
                ProtoLoadGeneratorTpchOutput::try_from(view)
                    .map_err(|_| {
                        TryFromProtoError::unknown_enum_variant("ProtoLoadGeneratorTpchOutput")
                    })?
                    .into_rust()?,
            ),
            None => {
                return Err(TryFromProtoError::missing_field(
                    "ProtoLoadGeneratorOutput::kind",
                ));
            }
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct LoadGeneratorSourceExportDetails {
    pub output: LoadGeneratorOutput,
}

impl RustType<ProtoLoadGeneratorSourceExportDetails> for LoadGeneratorSourceExportDetails {
    fn into_proto(&self) -> ProtoLoadGeneratorSourceExportDetails {
        ProtoLoadGeneratorSourceExportDetails {
            output: self.output.into_proto().into(),
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorSourceExportDetails) -> Result<Self, TryFromProtoError> {
        Ok(LoadGeneratorSourceExportDetails {
            output: proto
                .output
                .into_rust_if_some("ProtoLoadGeneratorSourceExportDetails::output")?,
        })
    }
}

impl AlterCompatible for LoadGeneratorSourceExportDetails {
    fn alter_compatible(&self, id: GlobalId, other: &Self) -> Result<(), AlterError> {
        let Self { output } = self;
        if output != &other.output {
            tracing::warn!(
                "LoadGeneratorSourceExportDetails incompatible at output:\nself:\n{:#?}\n\nother\n{:#?}",
                self,
                other
            );
            return Err(AlterError { id });
        }
        Ok(())
    }
}

pub trait Generator {
    /// Returns a function that produces rows and batch information.
    fn by_seed(
        &self,
        now: NowFn,
        seed: Option<u64>,
        resume_offset: MzOffset,
    ) -> Box<dyn Iterator<Item = (LoadGeneratorOutput, Event<Option<MzOffset>, (Row, Diff)>)>>;
}

impl RustType<ProtoLoadGeneratorSourceConnection> for LoadGeneratorSourceConnection {
    fn into_proto(&self) -> ProtoLoadGeneratorSourceConnection {
        use proto_load_generator_source_connection::Kind;
        ProtoLoadGeneratorSourceConnection {
            kind: Some(match &self.load_generator {
                LoadGenerator::Auction => Kind::Auction(()),
                LoadGenerator::Clock => Kind::Clock(()),
                LoadGenerator::Counter { max_cardinality } => {
                    Kind::Counter(ProtoCounterLoadGenerator {
                        max_cardinality: *max_cardinality,
                    })
                }
                LoadGenerator::Marketing => Kind::Marketing(()),
                LoadGenerator::Tpch {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                } => Kind::Tpch(ProtoTpchLoadGenerator {
                    count_supplier: *count_supplier,
                    count_part: *count_part,
                    count_customer: *count_customer,
                    count_orders: *count_orders,
                    count_clerk: *count_clerk,
                }),
                LoadGenerator::Datums => Kind::Datums(()),
                LoadGenerator::KeyValue(kv) => Kind::KeyValue(kv.into_proto()),
            }),
            tick_micros: self.tick_micros,
            as_of: self.as_of,
            up_to: self.up_to,
        }
    }

    fn from_proto(proto: ProtoLoadGeneratorSourceConnection) -> Result<Self, TryFromProtoError> {
        use proto_load_generator_source_connection::Kind;
        let kind = proto.kind.ok_or_else(|| {
            TryFromProtoError::missing_field("ProtoLoadGeneratorSourceConnection::kind")
        })?;
        Ok(LoadGeneratorSourceConnection {
            load_generator: match kind {
                Kind::Auction(()) => LoadGenerator::Auction,
                Kind::Clock(()) => LoadGenerator::Clock,
                Kind::Counter(ProtoCounterLoadGenerator { max_cardinality }) => {
                    LoadGenerator::Counter { max_cardinality }
                }
                Kind::Marketing(()) => LoadGenerator::Marketing,
                Kind::Tpch(ProtoTpchLoadGenerator {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                }) => LoadGenerator::Tpch {
                    count_supplier,
                    count_part,
                    count_customer,
                    count_orders,
                    count_clerk,
                },
                Kind::Datums(()) => LoadGenerator::Datums,
                Kind::KeyValue(kv) => LoadGenerator::KeyValue(kv.into_rust()?),
            },
            tick_micros: proto.tick_micros,
            as_of: proto.as_of,
            up_to: proto.up_to,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct KeyValueLoadGenerator {
    /// The keyspace of the source.
    pub keys: u64,
    /// The number of rounds to emit values for each key in the snapshot.
    /// This lets users scale the snapshot size independent of the keyspace.
    ///
    /// Please use `transactional_snapshot` and `non_transactional_snapshot_rounds`.
    pub snapshot_rounds: u64,
    /// When false, this lets us quickly produce updates, as opposed to a single-value
    /// per key during a transactional snapshot
    pub transactional_snapshot: bool,
    /// The number of random bytes for each value.
    pub value_size: u64,
    /// The number of partitions. The keyspace is spread evenly across the partitions.
    /// This lets users scale the concurrency of the source independently of the replica size.
    pub partitions: u64,
    /// If provided, the maximum rate at which new batches of updates, per-partition will be
    /// produced after the snapshot.
    pub tick_interval: Option<Duration>,
    /// The number of keys in each update batch.
    pub batch_size: u64,
    /// A per-source seed.
    pub seed: u64,
    /// Whether or not to include the offset in the value. The string is the column name.
    pub include_offset: Option<String>,
}

impl KeyValueLoadGenerator {
    /// The number of transactional snapshot rounds.
    pub fn transactional_snapshot_rounds(&self) -> u64 {
        if self.transactional_snapshot {
            self.snapshot_rounds
        } else {
            0
        }
    }

    /// The number of non-transactional snapshot rounds.
    pub fn non_transactional_snapshot_rounds(&self) -> u64 {
        if self.transactional_snapshot {
            0
        } else {
            self.snapshot_rounds
        }
    }
}

impl RustType<ProtoKeyValueLoadGenerator> for KeyValueLoadGenerator {
    fn into_proto(&self) -> ProtoKeyValueLoadGenerator {
        ProtoKeyValueLoadGenerator {
            keys: self.keys,
            snapshot_rounds: self.snapshot_rounds,
            transactional_snapshot: self.transactional_snapshot,
            value_size: self.value_size,
            partitions: self.partitions,
            tick_interval: self.tick_interval.into_proto(),
            batch_size: self.batch_size,
            seed: self.seed,
            include_offset: self.include_offset.clone(),
        }
    }

    fn from_proto(proto: ProtoKeyValueLoadGenerator) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            keys: proto.keys,
            snapshot_rounds: proto.snapshot_rounds,
            transactional_snapshot: proto.transactional_snapshot,
            value_size: proto.value_size,
            partitions: proto.partitions,
            tick_interval: proto.tick_interval.into_rust()?,
            batch_size: proto.batch_size,
            seed: proto.seed,
            include_offset: proto.include_offset,
        })
    }
}
