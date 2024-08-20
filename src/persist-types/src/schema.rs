// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persist schema evolution.

use std::sync::Arc;

use arrow::array::{new_null_array, Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, SchemaBuilder};
use itertools::Itertools;

/// Returns a function to migrate arrow data encoded by `old` to be the same
/// DataType as arrow data encoded by `new`, if `new` is backward compatible
/// with `old`. Exposed for testing.
pub fn backward_compatible(old: &DataType, new: &DataType) -> Option<Migration> {
    backward_compatible_typ(old, new).map(Migration)
}

/// See [backward_compatible].
pub struct Migration(ArrayMigration);

impl Migration {
    /// Returns true if the migration requires dropping data, including nested
    /// structs.
    pub fn contains_drop(&self) -> bool {
        self.0.contains_drop()
    }

    // TODO: fn preserves_order(&self) -> bool

    /// For the `old` and `new` schemas used at construction time, migrates data
    /// encoded by `old` to be the same arrow DataType as data encoded by `new`.
    pub fn migrate(&self, array: Arc<dyn Array>) -> Arc<dyn Array> {
        self.0.migrate(array)
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum ArrayMigration {
    NoOp,
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
    AlterFieldNullable {
        name: String,
    },
    Recurse {
        name: String,
        migration: ArrayMigration,
    },
}

impl ArrayMigration {
    fn contains_drop(&self) -> bool {
        use ArrayMigration::*;
        match self {
            NoOp => false,
            Struct(xs) => xs.iter().any(|x| x.contains_drop()),
        }
    }

    fn migrate(&self, array: Arc<dyn Array>) -> Arc<dyn Array> {
        use ArrayMigration::*;
        match self {
            NoOp => array,
            Struct(migrations) => {
                let len = array.len();
                let array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap_or_else(|| panic!("expected Struct got {:?}", array.data_type()))
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
            AlterFieldNullable { .. } => false,
            Recurse { migration, .. } => migration.contains_drop(),
        }
    }

    fn name(&self) -> &String {
        match self {
            StructArrayMigration::AddFieldNullable { name, .. } => name,
            StructArrayMigration::DropField { name } => name,
            StructArrayMigration::AlterFieldNullable { name } => name,
            StructArrayMigration::Recurse { name, .. } => name,
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
                let (idx, _) = fields
                    .find(name)
                    .unwrap_or_else(|| panic!("expected to find field {} in {:?}", name, fields));
                arrays.remove(idx);
                let mut f = SchemaBuilder::from(&*fields);
                f.remove(idx);
                *fields = f.finish().fields;
            }
            AlterFieldNullable { name } => {
                let (idx, _) = fields
                    .find(name)
                    .unwrap_or_else(|| panic!("expected to find field {} in {:?}", name, fields));
                let mut f = SchemaBuilder::from(&*fields);
                let field = f.field_mut(idx);
                // Defensively assert field is not nullable.
                assert_eq!(field.is_nullable(), false);
                *field = Arc::new(Field::new(field.name(), field.data_type().clone(), true));
                *fields = f.finish().fields;
            }
            Recurse { name, migration } => {
                let (idx, _) = fields
                    .find(name)
                    .unwrap_or_else(|| panic!("expected to find field {} in {:?}", name, fields));
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

fn backward_compatible_typ(old: &DataType, new: &DataType) -> Option<ArrayMigration> {
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

fn backward_compatible_struct(old: &Fields, new: &Fields) -> Option<ArrayMigration> {
    use itertools::EitherOrBoth::*;
    use ArrayMigration::*;
    use StructArrayMigration::*;

    // The common case is that fields will be in the same order, so start by
    // assuming that and match them up pairwise. If this assumption is
    // incorrect, then later there will be an add and drop for the same field
    // name and we'll detect it then.
    let old_new_fields = old
        .iter()
        .merge_join_by(new.iter(), |o, n| o.name().cmp(n.name()));

    let mut field_migrations = Vec::new();
    for x in old_new_fields {
        let (o, n) = match x {
            Both(o, n) => (o, n),
            Left(o) => {
                // Allowed to drop a field regardless of nullability.
                field_migrations.push(DropField {
                    name: o.name().to_owned(),
                });
                continue;
            }
            // Allowed to add a new field but it must be nullable.
            Right(n) if !n.is_nullable() => return None,
            Right(n) => {
                field_migrations.push(AddFieldNullable {
                    name: n.name().to_owned(),
                    typ: n.data_type().clone(),
                });
                continue;
            }
        };

        // Not allowed to make a nullable field into non-nullable.
        if o.is_nullable() && !n.is_nullable() {
            return None;
        }
        // However, allowed to make a non-nullable field nullable.
        let make_nullable = !o.is_nullable() && n.is_nullable();

        match backward_compatible_typ(o.data_type(), n.data_type()) {
            None => return None,
            Some(NoOp) if make_nullable => {
                field_migrations.push(AlterFieldNullable {
                    name: n.name().clone(),
                });
            }
            Some(NoOp) => continue,
            // For now, don't support both making a field nullable and also
            // modifying it in some other way. It doesn't seem that we need this for
            // mz usage.
            Some(_) if make_nullable => return None,
            Some(migration) => field_migrations.push(Recurse {
                name: n.name().clone(),
                migration,
            }),
        }
    }

    // Now detect if any re-ordering happened.
    field_migrations.sort_by(|x, y| x.name().cmp(y.name()));
    for (a, b) in field_migrations.iter().tuple_windows() {
        if a.name() == b.name() {
            return None;
        }
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

        // Add AND remove field in a struct.
        testcase(
            struct_([("a", Boolean, true)]),
            struct_([("b", Boolean, true)]),
            Some(true),
        );

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

        // For now, don't support both making a field nullable and also
        // modifying it in some other way. It doesn't seem that we need this for
        // mz usage.
        testcase(
            struct_([("a", struct_([]), false)]),
            struct_([("a", struct_([("b", Boolean, false)]), true)]),
            None,
        );

        // Similarly, don't support reordering fields. This matters to persist
        // because it affects sortedness, which is used in the consolidating
        // iter.
        testcase(
            struct_([("a", Boolean, false), ("b", Utf8, false)]),
            struct_([("b", Utf8, false), ("a", Boolean, false)]),
            None,
        );
    }
}
