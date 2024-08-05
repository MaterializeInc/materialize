// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist shard schema information.

use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{new_null_array, Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, SchemaBuilder};
use mz_ore::cast::CastFrom;
use mz_persist_types::columnar::Schema2;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

/// An ordered identifier for a pair of key and val schemas registered to a
/// shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
#[serde(try_from = "String", into = "String")]
pub struct SchemaId(pub(crate) usize);

impl std::fmt::Display for SchemaId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "h{}", self.0)
    }
}

impl From<SchemaId> for String {
    fn from(schema_id: SchemaId) -> Self {
        schema_id.to_string()
    }
}

impl TryFrom<String> for SchemaId {
    type Error = String;
    fn try_from(encoded: String) -> Result<Self, Self::Error> {
        let encoded = match encoded.strip_prefix('h') {
            Some(x) => x,
            None => return Err(format!("invalid SchemaId {}: incorrect prefix", encoded)),
        };
        let schema_id = u64::from_str(encoded)
            .map_err(|err| format!("invalid SchemaId {}: {}", encoded, err))?;
        Ok(SchemaId(usize::cast_from(schema_id)))
    }
}

/// Returns a function to migrate arrow data encoded by `old` to be the same
/// DataType as arrow data encoded by `new`, if `new` is backward compatible
/// with `old`. Exposed for testing.
pub fn backward_compatible<T, S: Schema2<T>>(
    old: &S,
    new: &S,
) -> Option<Box<dyn Fn(Arc<dyn Array>) -> Arc<dyn Array>>> {
    fn data_type<T>(schema: &impl Schema2<T>) -> DataType {
        use mz_persist_types::columnar::ColumnEncoder;
        let (array, _stats) = Schema2::encoder(schema).expect("valid schema").finish();
        Array::data_type(&array).clone()
    }
    let migration = backward_compatible_typ(&data_type(old), &data_type(new))?;
    Some(Box::new(move |old| migration.migrate(old)))
}

#[derive(Debug, PartialEq)]
pub(crate) enum ArrayMigration {
    NoOp,
    // TODO: MakeNullable,
    Struct(Vec<StructArrayMigration>),
    // TODO: Map -> List
}

#[derive(Debug, PartialEq)]
pub(crate) enum StructArrayMigration {
    AddFieldNullable {
        name: String,
        typ: DataType,
    },
    DropField {
        name: String,
    },
    Recurse {
        name: String,
        migration: ArrayMigration,
    },
}

impl ArrayMigration {
    /// Returns true if the migration requires dropping data, including nested
    /// structs.
    #[allow(dead_code)]
    pub(crate) fn contains_drop(&self) -> bool {
        use ArrayMigration::*;
        match self {
            NoOp => false,
            Struct(xs) => xs.iter().any(|x| x.contains_drop()),
        }
    }

    // TODO: fn preserves_order(&self) -> bool

    /// For the `old` and `new` schemas used at construction time, migrates data
    /// encoded by `old` to be the same arrow DataType as data encoded by `new`.
    pub(crate) fn migrate(&self, array: Arc<dyn Array>) -> Arc<dyn Array> {
        use ArrayMigration::*;
        match self {
            NoOp => array,
            Struct(migrations) => {
                let len = array.len();
                let array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap_or_else(|| {
                        panic!(
                            "internal error: expected Struct got {:?}",
                            array.data_type()
                        )
                    })
                    .clone();
                let (mut fields, mut arrays, nulls) = array.into_parts();
                for migration in migrations {
                    migration.migrate(len, &mut fields, &mut arrays);
                }
                Arc::new(StructArray::new(fields, arrays, nulls))
            }
        }
    }
}

impl StructArrayMigration {
    fn contains_drop(&self) -> bool {
        use StructArrayMigration::*;
        match self {
            AddFieldNullable { .. } => false,
            DropField { .. } => true,
            Recurse { migration, .. } => migration.contains_drop(),
        }
    }

    fn migrate(&self, len: usize, fields: &mut Fields, arrays: &mut Vec<Arc<dyn Array>>) {
        use StructArrayMigration::*;
        match self {
            AddFieldNullable { name, typ } => {
                arrays.push(new_null_array(typ, len));
                let mut f = SchemaBuilder::from(&*fields);
                f.push(Arc::new(Field::new(name, typ.clone(), true)));
                *fields = f.finish().fields;
            }
            DropField { name } => {
                let (idx, _) = fields.find(name).expect("WIP");
                arrays.remove(idx);
                let mut f = SchemaBuilder::from(&*fields);
                f.remove(idx);
                *fields = f.finish().fields;
            }
            Recurse { name, migration } => {
                let (idx, _) = fields.find(name).expect("WIP");
                arrays[idx] = migration.migrate(Arc::clone(&arrays[idx]));
                let mut f = SchemaBuilder::from(&*fields);
                *f.field_mut(idx) = Arc::new(Field::new(
                    name,
                    arrays[idx].data_type().clone(),
                    f.field(idx).is_nullable(),
                ));
                *fields = f.finish().fields;
            }
        }
    }
}

pub(crate) fn backward_compatible_typ(old: &DataType, new: &DataType) -> Option<ArrayMigration> {
    use ArrayMigration::NoOp;
    use DataType::*;
    match (old, new) {
        (Null, Null) => Some(NoOp),
        (Null, _) => None,
        (Boolean, Boolean) => Some(NoOp),
        (Boolean, _) => None,
        (Int8, Int8) => Some(NoOp),
        (Int8, _) => None,
        (Int16, Int16) => Some(NoOp),
        (Int16, _) => None,
        (Int32, Int32) => Some(NoOp),
        (Int32, _) => None,
        (Int64, Int64) => Some(NoOp),
        (Int64, _) => None,
        (UInt8, UInt8) => Some(NoOp),
        (UInt8, _) => None,
        (UInt16, UInt16) => Some(NoOp),
        (UInt16, _) => None,
        (UInt32, UInt32) => Some(NoOp),
        (UInt32, _) => None,
        (UInt64, UInt64) => Some(NoOp),
        (UInt64, _) => None,
        (Float16, Float16) => Some(NoOp),
        (Float16, _) => None,
        (Float32, Float32) => Some(NoOp),
        (Float32, _) => None,
        (Float64, Float64) => Some(NoOp),
        (Float64, _) => None,
        (Binary, Binary) => Some(NoOp),
        (Binary, _) => None,
        (FixedSizeBinary(o), FixedSizeBinary(n)) => (o == n).then_some(NoOp),
        (FixedSizeBinary(_), _) => None,
        (Utf8, Utf8) => Some(NoOp),
        (Utf8, _) => None,
        (Struct(o), Struct(n)) => backward_compatible_struct(o, n),
        (Struct(_), _) => None,
        (List(o), List(n)) => (o == n).then_some(NoOp),
        (List(_), _) => None,
        (Map(o, _), Map(n, _)) => (o == n).then_some(NoOp),
        (Map(_, _), _) => None,
        (
            Timestamp(_, _)
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Duration(_)
            | Interval(_)
            | LargeBinary
            | BinaryView
            | LargeUtf8
            | Utf8View
            | ListView(_)
            | FixedSizeList(_, _)
            | LargeList(_)
            | LargeListView(_)
            | Union(_, _)
            | Dictionary(_, _)
            | Decimal128(_, _)
            | Decimal256(_, _)
            | RunEndEncoded(_, _),
            _,
        ) => unimplemented!("not used in mz: old={:?} new={:?}", old, new),
    }
}

pub(crate) fn backward_compatible_field(old: &Field, new: &Field) -> Option<ArrayMigration> {
    // Not allowed to make a nullable field into non-nullable.
    if old.is_nullable() && !new.is_nullable() {
        return None;
    }
    // WIP make this work
    if !old.is_nullable() && new.is_nullable() {
        return None;
    }
    backward_compatible_typ(old.data_type(), new.data_type())
}

pub(crate) fn backward_compatible_struct(old: &Fields, new: &Fields) -> Option<ArrayMigration> {
    use ArrayMigration::*;
    use StructArrayMigration::*;
    let mut field_migrations = Vec::new();

    for n in new.iter() {
        // WIP not n^2
        let Some((_, o)) = old.find(n.name()) else {
            // Allowed to add a new field but must be nullable.
            if n.is_nullable() {
                field_migrations.push(AddFieldNullable {
                    name: n.name().to_owned(),
                    typ: n.data_type().clone(),
                });
                continue;
            } else {
                return None;
            }
        };
        match backward_compatible_field(o, n) {
            None => return None,
            Some(NoOp) => continue,
            Some(migration) => field_migrations.push(Recurse {
                name: n.name().clone(),
                migration,
            }),
        }
    }

    // Also drop any fields in old but not in new
    for o in old.iter() {
        // WIP not n^2
        if new.find(o.name()).is_some() {
            continue;
        }
        field_migrations.push(DropField {
            name: o.name().to_owned(),
        });
    }

    if field_migrations.is_empty() {
        Some(NoOp)
    } else {
        Some(Struct(field_migrations))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::new_empty_array;
    use arrow::datatypes::Field;

    use super::*;

    #[mz_ore::test]
    fn schema_id() {
        assert_eq!(SchemaId(1).to_string(), "h1");
        assert_eq!(SchemaId::try_from("h1".to_owned()), Ok(SchemaId(1)));
        assert!(SchemaId::try_from("nope".to_owned()).is_err());
    }

    #[mz_ore::test]
    fn backward_compatible() {
        use DataType::*;

        #[track_caller]
        fn testcase(old: DataType, new: DataType, expected: Option<bool>) {
            let migration = super::backward_compatible_typ(&old, &new);
            let actual = migration.as_ref().map(|x| x.contains_drop());
            assert_eq!(actual, expected);
            // If it's backward compatible, make sure that the migration
            // logic works.
            if let Some(migration) = migration {
                let (old, new) = (new_empty_array(&old), new_empty_array(&new));
                let migrated = migration.migrate(old);
                if let Struct(_) = migrated.data_type() {
                    // WIP implement the migration to make the field nullable.
                    return;
                }
                assert_eq!(new.data_type(), migrated.data_type());
            }
        }
        fn struct_(fields: impl IntoIterator<Item = (&'static str, DataType, bool)>) -> DataType {
            let fields = fields
                .into_iter()
                .map(|(name, typ, nullable)| Field::new(name, typ, nullable))
                .collect();
            DataType::Struct(fields)
        }

        // Matching primitive types
        testcase(Boolean, Boolean, Some(false));
        testcase(Utf8, Utf8, Some(false));

        // Non-matching primitive types
        testcase(Boolean, Utf8, None);
        testcase(Utf8, Boolean, None);

        // Matching structs.
        testcase(
            struct_([("a", Boolean, true)]),
            struct_([("a", Boolean, true)]),
            Some(false),
        );
        testcase(
            struct_([("a", Boolean, false)]),
            struct_([("a", Boolean, false)]),
            Some(false),
        );

        // Changing nullability in a struct.
        testcase(
            struct_([("a", Boolean, true)]),
            struct_([("a", Boolean, false)]),
            None,
        );
        testcase(
            struct_([("a", Boolean, false)]),
            struct_([("a", Boolean, true)]),
            Some(false),
        );

        // Add/remove field in a struct.
        testcase(struct_([]), struct_([("a", Boolean, true)]), Some(false));
        testcase(struct_([]), struct_([("a", Boolean, false)]), None);
        testcase(struct_([("a", Boolean, true)]), struct_([]), Some(true));
        testcase(struct_([("a", Boolean, false)]), struct_([]), Some(true));

        // Nested struct.
        testcase(
            struct_([("a", struct_([("b", Boolean, false)]), false)]),
            struct_([("a", struct_([("b", Boolean, false)]), false)]),
            Some(false),
        );
        testcase(
            struct_([("a", struct_([]), false)]),
            struct_([("a", struct_([("b", Boolean, true)]), false)]),
            Some(false),
        );
        testcase(
            struct_([("a", struct_([]), false)]),
            struct_([("a", struct_([("b", Boolean, false)]), false)]),
            None,
        );
        testcase(
            struct_([("a", struct_([("b", Boolean, true)]), false)]),
            struct_([("a", struct_([]), false)]),
            Some(true),
        );
        testcase(
            struct_([("a", struct_([("b", Boolean, false)]), false)]),
            struct_([("a", struct_([]), false)]),
            Some(true),
        );
    }
}
