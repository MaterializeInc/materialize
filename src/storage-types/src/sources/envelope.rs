// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types related to source envelopes

use anyhow::{anyhow, bail};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{RelationDesc, SqlColumnType, SqlRelationType, SqlScalarType};
use proptest::prelude::any;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.sources.envelope.rs"
));

/// `SourceEnvelope`s describe how to turn a stream of messages from `SourceDesc`s
/// into a _differential stream_, that is, a stream of (data, time, diff)
/// triples.
///
/// PostgreSQL sources skip any explicit envelope handling, effectively
/// asserting that `SourceEnvelope` is `None` with `KeyEnvelope::None`.
#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum SourceEnvelope {
    /// The most trivial version is `None`, which typically produces triples where the diff
    /// is `1`. However, some sources are able to produce values with more exotic diff's,
    /// such as the posgres source. Currently, this is the only variant usable with
    /// those sources.
    ///
    /// If the `KeyEnvelope` is present,
    /// include the key columns as an output column of the source with the given properties.
    None(NoneEnvelope),
    /// `Upsert` holds onto previously seen values and produces `1` or `-1` diffs depending on
    /// whether or not the required _key_ outputed by the source has been seen before. This also
    /// supports a `Debezium` mode.
    Upsert(UpsertEnvelope),
    /// `CdcV2` requires sources output messages in a strict form that requires a upstream-provided
    /// timeline.
    CdcV2,
}

impl RustType<ProtoSourceEnvelope> for SourceEnvelope {
    fn into_proto(&self) -> ProtoSourceEnvelope {
        use proto_source_envelope::Kind;
        ProtoSourceEnvelope {
            kind: Some(match self {
                SourceEnvelope::None(e) => Kind::None(e.into_proto()),
                SourceEnvelope::Upsert(e) => Kind::Upsert(e.into_proto()),
                SourceEnvelope::CdcV2 => Kind::CdcV2(()),
            }),
        }
    }

    fn from_proto(proto: ProtoSourceEnvelope) -> Result<Self, TryFromProtoError> {
        use proto_source_envelope::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoSourceEnvelope::kind"))?;
        Ok(match kind {
            Kind::None(e) => SourceEnvelope::None(e.into_rust()?),
            Kind::Upsert(e) => SourceEnvelope::Upsert(e.into_rust()?),
            Kind::CdcV2(()) => SourceEnvelope::CdcV2,
        })
    }
}

/// `UnplannedSourceEnvelope` is a `SourceEnvelope` missing some information. This information
/// is obtained in `UnplannedSourceEnvelope::desc`, where
/// `UnplannedSourceEnvelope::into_source_envelope`
/// creates a full `SourceEnvelope`
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UnplannedSourceEnvelope {
    None(KeyEnvelope),
    Upsert { style: UpsertStyle },
    CdcV2,
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct NoneEnvelope {
    pub key_envelope: KeyEnvelope,
    pub key_arity: usize,
}

impl RustType<ProtoNoneEnvelope> for NoneEnvelope {
    fn into_proto(&self) -> ProtoNoneEnvelope {
        ProtoNoneEnvelope {
            key_envelope: Some(self.key_envelope.into_proto()),
            key_arity: self.key_arity.into_proto(),
        }
    }

    fn from_proto(proto: ProtoNoneEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(NoneEnvelope {
            key_envelope: proto
                .key_envelope
                .into_rust_if_some("ProtoNoneEnvelope::key_envelope")?,
            key_arity: proto.key_arity.into_rust()?,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct UpsertEnvelope {
    /// Full arity, including the key columns
    pub source_arity: usize,
    /// What style of Upsert we are using
    pub style: UpsertStyle,
    /// The indices of the keys in the full value row, used
    /// to deduplicate data in `upsert_core`
    #[proptest(strategy = "proptest::collection::vec(any::<usize>(), 0..4)")]
    pub key_indices: Vec<usize>,
}

impl RustType<ProtoUpsertEnvelope> for UpsertEnvelope {
    fn into_proto(&self) -> ProtoUpsertEnvelope {
        ProtoUpsertEnvelope {
            source_arity: self.source_arity.into_proto(),
            style: Some(self.style.into_proto()),
            key_indices: self.key_indices.into_proto(),
        }
    }

    fn from_proto(proto: ProtoUpsertEnvelope) -> Result<Self, TryFromProtoError> {
        Ok(UpsertEnvelope {
            source_arity: proto.source_arity.into_rust()?,
            style: proto
                .style
                .into_rust_if_some("ProtoUpsertEnvelope::style")?,
            key_indices: proto.key_indices.into_rust()?,
        })
    }
}

#[derive(Arbitrary, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum UpsertStyle {
    /// `ENVELOPE UPSERT`, where the key shape depends on the independent
    /// `KeyEnvelope`
    Default(KeyEnvelope),
    /// `ENVELOPE DEBEZIUM UPSERT`
    Debezium { after_idx: usize },
    /// `ENVELOPE UPSERT` where any decode errors will get serialized into a
    /// SqlScalarType::Record column named `error_column`, and all value columns are
    /// nullable. The key shape depends on the independent `KeyEnvelope`.
    ValueErrInline {
        key_envelope: KeyEnvelope,
        error_column: String,
    },
}

impl RustType<ProtoUpsertStyle> for UpsertStyle {
    fn into_proto(&self) -> ProtoUpsertStyle {
        use proto_upsert_style::{Kind, ProtoDebezium, ProtoValueErrInline};
        ProtoUpsertStyle {
            kind: Some(match self {
                UpsertStyle::Default(e) => Kind::Default(e.into_proto()),
                UpsertStyle::Debezium { after_idx } => Kind::Debezium(ProtoDebezium {
                    after_idx: after_idx.into_proto(),
                }),
                UpsertStyle::ValueErrInline {
                    key_envelope,
                    error_column,
                } => Kind::ValueErrorInline(ProtoValueErrInline {
                    key_envelope: Some(key_envelope.into_proto()),
                    error_column: error_column.clone(),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoUpsertStyle) -> Result<Self, TryFromProtoError> {
        use proto_upsert_style::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoUpsertStyle::kind"))?;
        Ok(match kind {
            Kind::Default(e) => UpsertStyle::Default(e.into_rust()?),
            Kind::Debezium(d) => UpsertStyle::Debezium {
                after_idx: d.after_idx.into_rust()?,
            },
            Kind::ValueErrorInline(e) => UpsertStyle::ValueErrInline {
                key_envelope: e
                    .key_envelope
                    .ok_or_else(|| {
                        TryFromProtoError::missing_field("ProtoValueErrInline::key_envelope")
                    })?
                    .into_rust()?,
                error_column: e.error_column.clone(),
            },
        })
    }
}

/// Computes the indices of the value's relation description that appear in the key.
///
/// Returns an error if it detects a common columns between the two relations that has the same
/// name but a different type, if a key column is missing from the value, and if the key relation
/// has a column with no name.
fn match_key_indices(
    key_desc: &RelationDesc,
    value_desc: &RelationDesc,
) -> anyhow::Result<Vec<usize>> {
    let mut indices = Vec::new();
    for (name, key_type) in key_desc.iter() {
        let (index, value_type) = value_desc
            .get_by_name(name)
            .ok_or_else(|| anyhow!("Value schema missing primary key column: {}", name))?;

        if key_type == value_type {
            indices.push(index);
        } else {
            bail!(
                "key and value column types do not match: key {:?} vs. value {:?}",
                key_type,
                value_type
            );
        }
    }
    Ok(indices)
}

impl UnplannedSourceEnvelope {
    /// Transforms an `UnplannedSourceEnvelope` into a `SourceEnvelope`
    ///
    /// Panics if the input envelope is `UnplannedSourceEnvelope::Upsert` and
    /// key is not passed as `Some`
    // TODO(petrosagg): This API looks very error prone. Can we statically enforce it somehow?
    fn into_source_envelope(
        self,
        key: Option<Vec<usize>>,
        key_arity: Option<usize>,
        source_arity: Option<usize>,
    ) -> SourceEnvelope {
        match self {
            UnplannedSourceEnvelope::Upsert {
                style: upsert_style,
            } => SourceEnvelope::Upsert(UpsertEnvelope {
                style: upsert_style,
                key_indices: key.expect(
                    "into_source_envelope to be passed \
                    correct parameters for UnplannedSourceEnvelope::Upsert",
                ),
                source_arity: source_arity.expect(
                    "into_source_envelope to be passed \
                    correct parameters for UnplannedSourceEnvelope::Upsert",
                ),
            }),
            UnplannedSourceEnvelope::None(key_envelope) => SourceEnvelope::None(NoneEnvelope {
                key_envelope,
                key_arity: key_arity.unwrap_or(0),
            }),
            UnplannedSourceEnvelope::CdcV2 => SourceEnvelope::CdcV2,
        }
    }

    /// Computes the output relation of this envelope when applied on top of the decoded key and
    /// value relation desc
    pub fn desc(
        self,
        key_desc: Option<RelationDesc>,
        value_desc: RelationDesc,
        metadata_desc: RelationDesc,
    ) -> anyhow::Result<(SourceEnvelope, RelationDesc)> {
        Ok(match &self {
            UnplannedSourceEnvelope::None(key_envelope)
            | UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Default(key_envelope),
                ..
            }
            | UnplannedSourceEnvelope::Upsert {
                style:
                    UpsertStyle::ValueErrInline {
                        key_envelope,
                        error_column: _,
                    },
            } => {
                let (key_arity, key_desc) = match key_desc {
                    Some(desc) if !desc.is_empty() => (Some(desc.arity()), Some(desc)),
                    _ => (None, None),
                };

                // Compute any key relation and key indices
                let (key_desc, key) = match (key_desc, key_arity, key_envelope) {
                    (_, _, KeyEnvelope::None) => (None, None),
                    (Some(key_desc), Some(key_arity), KeyEnvelope::Flattened) => {
                        // Add the key columns as a key.
                        let key_indices: Vec<usize> = (0..key_arity).collect();
                        let key_desc = key_desc.with_key(key_indices.clone());
                        (Some(key_desc), Some(key_indices))
                    }
                    (Some(key_desc), Some(key_arity), KeyEnvelope::Named(key_name)) => {
                        let key_desc = {
                            // if the key has multiple objects, nest them as a record inside of a single name
                            if key_arity > 1 {
                                let key_type = key_desc.typ();
                                let key_as_record = SqlRelationType::new(vec![SqlColumnType {
                                    nullable: false,
                                    scalar_type: SqlScalarType::Record {
                                        fields: key_desc
                                            .iter_names()
                                            .zip(key_type.column_types.iter())
                                            .map(|(name, ty)| (name.clone(), ty.clone()))
                                            .collect(),
                                        custom_id: None,
                                    },
                                }]);

                                RelationDesc::new(key_as_record, [key_name.to_string()])
                            } else {
                                key_desc.with_names([key_name.to_string()])
                            }
                        };
                        let (key_desc, key) = match self {
                            UnplannedSourceEnvelope::None(_) => (key_desc, None),
                            // If we're applying the upsert logic the key column will be unique
                            UnplannedSourceEnvelope::Upsert { .. } => {
                                (key_desc.with_key(vec![0]), Some(vec![0]))
                            }
                            _ => unreachable!(),
                        };
                        (Some(key_desc), key)
                    }
                    (None, _, _) => (None, None),
                    (_, None, _) => (None, None),
                };

                let value_desc = compute_envelope_value_desc(&self, value_desc);
                // Add value-related columns and metadata columns after any key columns.
                let desc = match key_desc {
                    Some(key_desc) => key_desc.concat(value_desc).concat(metadata_desc),
                    None => value_desc.concat(metadata_desc),
                };
                (
                    self.into_source_envelope(key, key_arity, Some(desc.arity())),
                    desc,
                )
            }
            UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Debezium { after_idx },
                ..
            } => match &value_desc.typ().column_types[*after_idx].scalar_type {
                SqlScalarType::Record { fields, .. } => {
                    let mut desc = RelationDesc::from_names_and_types(fields.clone());
                    let key = key_desc.map(|k| match_key_indices(&k, &desc)).transpose()?;
                    if let Some(key) = key.clone() {
                        desc = desc.with_key(key);
                    }

                    let desc = match self {
                        UnplannedSourceEnvelope::Upsert { .. } => desc.concat(metadata_desc),
                        _ => desc,
                    };

                    (
                        self.into_source_envelope(key, None, Some(desc.arity())),
                        desc,
                    )
                }
                ty => bail!(
                    "Incorrect type for Debezium value, expected Record, got {:?}",
                    ty
                ),
            },
            UnplannedSourceEnvelope::CdcV2 => {
                // the correct types

                // CdcV2 row data are in a record in a record in a list
                match &value_desc.typ().column_types[0].scalar_type {
                    SqlScalarType::List { element_type, .. } => match &**element_type {
                        SqlScalarType::Record { fields, .. } => {
                            // TODO maybe check this by name
                            match &fields[0].1.scalar_type {
                                SqlScalarType::Record { fields, .. } => (
                                    self.into_source_envelope(None, None, None),
                                    RelationDesc::from_names_and_types(fields.clone()),
                                ),
                                ty => {
                                    bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty)
                                }
                            }
                        }
                        ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                    },
                    ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                }
            }
        })
    }
}

/// Whether and how to include the decoded key of a stream in dataflows
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum KeyEnvelope {
    /// Never include the key in the output row
    None,
    /// For composite key encodings, pull the fields from the encoding into columns.
    Flattened,
    /// Always use the given name for the key.
    ///
    /// * For a single-field key, this means that the column will get the given name.
    /// * For a multi-column key, the columns will get packed into a [`SqlScalarType::Record`], and
    ///   that Record will get the given name.
    Named(String),
}

impl RustType<ProtoKeyEnvelope> for KeyEnvelope {
    fn into_proto(&self) -> ProtoKeyEnvelope {
        use proto_key_envelope::Kind;
        ProtoKeyEnvelope {
            kind: Some(match self {
                KeyEnvelope::None => Kind::None(()),
                KeyEnvelope::Flattened => Kind::Flattened(()),
                KeyEnvelope::Named(name) => Kind::Named(name.clone()),
            }),
        }
    }

    fn from_proto(proto: ProtoKeyEnvelope) -> Result<Self, TryFromProtoError> {
        use proto_key_envelope::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoKeyEnvelope::kind"))?;
        Ok(match kind {
            Kind::None(()) => KeyEnvelope::None,
            Kind::Flattened(()) => KeyEnvelope::Flattened,
            Kind::Named(name) => KeyEnvelope::Named(name),
        })
    }
}

/// Compute the resulting value relation given the decoded value relation and the envelope
/// style. If the ValueErrInline upsert style is used this will add an error column to the
/// beginning of the relation and make all value fields nullable.
fn compute_envelope_value_desc(
    source_envelope: &UnplannedSourceEnvelope,
    value_desc: RelationDesc,
) -> RelationDesc {
    match &source_envelope {
        UnplannedSourceEnvelope::Upsert {
            style:
                UpsertStyle::ValueErrInline {
                    key_envelope: _,
                    error_column,
                },
        } => {
            let mut names = Vec::with_capacity(value_desc.arity() + 1);
            names.push(error_column.as_str().into());
            names.extend(value_desc.iter_names().cloned());

            let mut types = Vec::with_capacity(value_desc.arity() + 1);
            types.push(SqlColumnType {
                nullable: true,
                scalar_type: SqlScalarType::Record {
                    fields: [(
                        "description".into(),
                        SqlColumnType {
                            nullable: true,
                            scalar_type: SqlScalarType::String,
                        },
                    )]
                    .into(),
                    custom_id: None,
                },
            });
            types.extend(value_desc.iter_types().map(|t| t.clone().nullable(true)));
            let relation_type = SqlRelationType::new(types);
            RelationDesc::new(relation_type, names)
        }
        _ => value_desc,
    }
}
