// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning functions related to running MySQL ingestions.

use std::collections::BTreeMap;
use std::time::Duration;

use itertools::Itertools;
use mz_ore::cast::TryCastFrom;
use mz_ore::str::StrExt;
use mz_repr::RelationDesc;
use mz_sql_parser::ast::{
    self, CreateSourceConnection, CreateSourceStatement, LoadGeneratorOption,
    LoadGeneratorOptionName, SourceEnvelope, SourceIncludeMetadata,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sources::load_generator::{
    KeyValueLoadGenerator, LoadGenerator, LoadGeneratorSourceConnection,
    LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT,
};
use mz_storage_types::sources::{GenericSourceConnection, SourceDesc};

use crate::ast::display::AstDisplay;
use crate::names::{Aug, FullItemName, RawDatabaseSpecifier};
use crate::plan::error::PlanError;
use crate::plan::plan_utils;
use crate::plan::statement::StatementContext;
use crate::plan::with_options::TryFromValue;
use crate::session::vars;

generate_extracted_config!(
    LoadGeneratorOption,
    (TickInterval, Duration),
    (ScaleFactor, f64),
    (MaxCardinality, u64),
    (Keys, u64),
    (SnapshotRounds, u64),
    (TransactionalSnapshot, bool),
    (ValueSize, u64),
    (Seed, u64),
    (Partitions, u64),
    (BatchSize, u64)
);

impl LoadGeneratorOptionExtracted {
    pub(super) fn ensure_only_valid_options(
        &self,
        loadgen: &ast::LoadGenerator,
    ) -> Result<(), PlanError> {
        use mz_sql_parser::ast::LoadGeneratorOptionName::*;

        let mut options = self.seen.clone();

        let permitted_options: &[_] = match loadgen {
            ast::LoadGenerator::Auction => &[TickInterval],
            ast::LoadGenerator::Counter => &[TickInterval, MaxCardinality],
            ast::LoadGenerator::Marketing => &[TickInterval],
            ast::LoadGenerator::Datums => &[TickInterval],
            ast::LoadGenerator::Tpch => &[TickInterval, ScaleFactor],
            ast::LoadGenerator::KeyValue => &[
                TickInterval,
                Keys,
                SnapshotRounds,
                TransactionalSnapshot,
                ValueSize,
                Seed,
                Partitions,
                BatchSize,
            ],
        };

        for o in permitted_options {
            options.remove(o);
        }

        if !options.is_empty() {
            sql_bail!(
                "{} load generators do not support {} values",
                loadgen,
                options.iter().join(", ")
            )
        }

        Ok(())
    }
}

pub(crate) fn load_generator_ast_to_generator(
    scx: &StatementContext,
    loadgen: &ast::LoadGenerator,
    options: &[LoadGeneratorOption<Aug>],
    include_metadata: &[SourceIncludeMetadata],
) -> Result<
    (
        LoadGenerator,
        Option<BTreeMap<FullItemName, (usize, RelationDesc)>>,
    ),
    PlanError,
> {
    let extracted: LoadGeneratorOptionExtracted = options.to_vec().try_into()?;
    extracted.ensure_only_valid_options(loadgen)?;

    if loadgen != &ast::LoadGenerator::KeyValue && !include_metadata.is_empty() {
        sql_bail!("INCLUDE metadata only supported with `KEY VALUE` load generators");
    }

    let load_generator = match loadgen {
        ast::LoadGenerator::Auction => LoadGenerator::Auction,
        ast::LoadGenerator::Counter => {
            let LoadGeneratorOptionExtracted {
                max_cardinality, ..
            } = extracted;
            LoadGenerator::Counter { max_cardinality }
        }
        ast::LoadGenerator::Marketing => LoadGenerator::Marketing,
        ast::LoadGenerator::Datums => LoadGenerator::Datums,
        ast::LoadGenerator::Tpch => {
            let LoadGeneratorOptionExtracted { scale_factor, .. } = extracted;

            // Default to 0.01 scale factor (=10MB).
            let sf: f64 = scale_factor.unwrap_or(0.01);
            if !sf.is_finite() || sf < 0.0 {
                sql_bail!("unsupported scale factor {sf}");
            }

            let f_to_i = |multiplier: f64| -> Result<i64, PlanError> {
                let total = (sf * multiplier).floor();
                let mut i = i64::try_cast_from(total)
                    .ok_or_else(|| sql_err!("unsupported scale factor {sf}"))?;
                if i < 1 {
                    i = 1;
                }
                Ok(i)
            };

            // The multiplications here are safely unchecked because they will
            // overflow to infinity, which will be caught by f64_to_i64.
            let count_supplier = f_to_i(10_000f64)?;
            let count_part = f_to_i(200_000f64)?;
            let count_customer = f_to_i(150_000f64)?;
            let count_orders = f_to_i(150_000f64 * 10f64)?;
            let count_clerk = f_to_i(1_000f64)?;

            LoadGenerator::Tpch {
                count_supplier,
                count_part,
                count_customer,
                count_orders,
                count_clerk,
            }
        }
        mz_sql_parser::ast::LoadGenerator::KeyValue => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_KEY_VALUE)?;
            let LoadGeneratorOptionExtracted {
                keys,
                snapshot_rounds,
                transactional_snapshot,
                value_size,
                tick_interval,
                seed,
                partitions,
                batch_size,
                ..
            } = extracted;

            let mut include_offset = None;
            for im in include_metadata {
                match im {
                    SourceIncludeMetadata::Offset { alias } => {
                        include_offset = match alias {
                            Some(alias) => Some(alias.to_string()),
                            None => Some(LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT.to_string()),
                        }
                    }
                    SourceIncludeMetadata::Key { .. } => continue,

                    _ => {
                        sql_bail!("only `INCLUDE OFFSET` and `INCLUDE KEY` is supported");
                    }
                };
            }

            let lgkv = KeyValueLoadGenerator {
                keys: keys.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires KEYS"))?,
                snapshot_rounds: snapshot_rounds
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SNAPSHOT ROUNDS"))?,
                // Defaults to true.
                transactional_snapshot: transactional_snapshot.unwrap_or(true),
                value_size: value_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires VALUE SIZE"))?,
                partitions: partitions
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires PARTITIONS"))?,
                tick_interval,
                batch_size: batch_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires BATCH SIZE"))?,
                seed: seed.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SEED"))?,
                include_offset,
            };

            if lgkv.keys == 0
                || lgkv.partitions == 0
                || lgkv.value_size == 0
                || lgkv.batch_size == 0
            {
                sql_bail!("LOAD GENERATOR KEY VALUE options must be non-zero")
            }

            if lgkv.keys % lgkv.partitions != 0 {
                sql_bail!("KEYS must be a multiple of PARTITIONS")
            }

            if lgkv.batch_size > lgkv.keys {
                sql_bail!("KEYS must be larger than BATCH SIZE")
            }

            // This constraints simplifies the source implementation.
            // We can lift it later.
            if (lgkv.keys / lgkv.partitions) % lgkv.batch_size != 0 {
                sql_bail!("PARTITIONS * BATCH SIZE must be a divisor of KEYS")
            }

            if lgkv.snapshot_rounds == 0 {
                sql_bail!("SNAPSHOT ROUNDS must be larger than 0")
            }

            LoadGenerator::KeyValue(lgkv)
        }
    };

    let mut available_subsources = BTreeMap::new();
    for (i, (name, desc)) in load_generator.views().iter().enumerate() {
        let name = FullItemName {
            database: RawDatabaseSpecifier::Name(
                mz_storage_types::sources::load_generator::LOAD_GENERATOR_DATABASE_NAME.to_owned(),
            ),
            schema: load_generator.schema_name().into(),
            item: name.to_string(),
        };
        // The zero-th output is the main output
        // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
        // to handle the main source and the subsources uniformly
        available_subsources.insert(name, (i + 1, desc.clone()));
    }
    let available_subsources = if available_subsources.is_empty() {
        None
    } else {
        Some(available_subsources)
    };

    Ok((load_generator, available_subsources))
}

pub(super) fn plan_create_source_desc_load_gen(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    let CreateSourceStatement {
        name,
        in_cluster: _,
        col_names,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    let CreateSourceConnection::LoadGenerator { generator, options } = connection else {
        panic!("must be Load Generator connection")
    };

    for (check, feature) in [
        (
            matches!(envelope, Some(SourceEnvelope::None) | None)
                || mz_sql_parser::ast::LoadGenerator::KeyValue == *generator,
            "ENVELOPE other than NONE (except for KEY VALUE)",
        ),
        (format.is_none(), "FORMAT"),
    ] {
        if !check {
            bail_never_supported!(format!("{} with load generator source", feature));
        }
    }

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    let (load_generator, _available_subsources) =
        load_generator_ast_to_generator(scx, generator, options, include_metadata)?;

    let LoadGeneratorOptionExtracted { tick_interval, .. } = options.clone().try_into()?;
    let tick_micros = match tick_interval {
        Some(interval) => Some(interval.as_micros().try_into()?),
        None => None,
    };

    let connection =
        GenericSourceConnection::<ReferencedConnection>::from(LoadGeneratorSourceConnection {
            load_generator,
            tick_micros,
        });

    let (encoding, envelope, desc) =
        super::plan_encoding_envelope(scx, &connection, format, &envelope, include_metadata)?;

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection,
        encoding,
        envelope,
    };

    Ok((source_desc, desc))
}
