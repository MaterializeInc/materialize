// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type erased columnar structs.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, BooleanBufferBuilder, NullArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Fields};
use mz_ore::assert_none;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{ColumnGet, ColumnPush, Data, DataType};
use crate::dyn_col::{DynColumnMut, DynColumnRef};
use crate::stats::{OptionStats, StatsFn, StructStats};

/// The schema of a "dynamic" columnar struct.
///
/// Here, _dynamic_ is used to mean that, unlike most [Data] impls, the columnar
/// schema of this type is not inferrable purely from the type. Instead, it
/// needs supplementary [crate::columnar::ColumnCfg].
///
/// This exists because, among other reasons, mz's `Row` type does not declare
/// the schema at the type level (as opposed to something like `Row(u64, i32)`).
///
/// TODO(parkmycar): Remove this and all `DynStruct*` friends once
/// [`crate::columnar::Schema2`] is fully flushed out.
#[derive(Debug, Clone)]
#[cfg_attr(debug_assertions, derive(PartialEq))]
pub struct DynStructCfg {
    pub(crate) cols: Arc<Vec<(String, DataType, StatsFn)>>,
}

impl From<Vec<(String, DataType, StatsFn)>> for DynStructCfg {
    fn from(value: Vec<(String, DataType, StatsFn)>) -> Self {
        DynStructCfg {
            cols: Arc::new(value),
        }
    }
}

/// A "dynamic" columnar struct.
///
/// See [DynStructCfg].
#[derive(Debug)]
pub struct DynStruct;

/// The [Data::Ref] type for [DynStruct].
#[derive(Debug)]
pub enum DynStructRef<'a> {
    /// A sentinel to push the default value of each field in the struct.
    Default,
    /// A specific offset in a struct column.
    Idx {
        /// The offset.
        idx: usize,
        /// The fields of the struct column being referenced.
        cols: &'a [DynColumnRef],
    },
}

impl Default for DynStructRef<'_> {
    fn default() -> Self {
        DynStructRef::Default
    }
}

/// A [crate::columnar::ColumnGet] impl for [DynStruct].
#[derive(Debug)]
pub struct DynStructCol {
    pub(crate) len: usize,
    pub(crate) cfg: DynStructCfg,
    pub(crate) validity: Option<NullBuffer>,
    pub(crate) cols: Vec<DynColumnRef>,
}

impl ColumnRef<DynStructCfg> for DynStructCol {
    fn cfg(&self) -> &DynStructCfg {
        &self.cfg
    }

    fn len(&self) -> usize {
        self.len
    }

    fn to_arrow(&self) -> Arc<dyn Array> {
        let array: Arc<dyn Array> = match self.to_arrow_struct() {
            Some(array) => Arc::new(array),
            None => Arc::new(NullArray::new(self.len)),
        };
        array
    }

    fn from_arrow(cfg: &DynStructCfg, array: &dyn Array) -> Result<Self, String> {
        Self::from_arrow(cfg.clone(), array)
    }
}

impl ColumnGet<DynStruct> for DynStructCol {
    fn get<'a>(&'a self, idx: usize) -> DynStructRef<'a> {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsRef, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        assert_none!(self.validity);
        DynStructRef::Idx {
            idx,
            cols: &self.cols,
        }
    }
}

impl ColumnGet<Option<DynStruct>> for DynStructCol {
    fn get<'a>(&'a self, idx: usize) -> Option<DynStructRef<'a>> {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsRef, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        if self.validity.as_ref().map_or(true, |x| x.is_valid(idx)) {
            Some(DynStructRef::Idx {
                idx,
                cols: &self.cols,
            })
        } else {
            None
        }
    }
}

impl DynStructCol {
    pub(crate) fn empty(cfg: DynStructCfg) -> Self {
        DynStructCol {
            len: 0,
            cfg,
            validity: None,
            cols: Vec::new(),
        }
    }

    fn cols(&self) -> impl Iterator<Item = (&str, &StatsFn, &DynColumnRef)> {
        debug_assert_eq!(self.cfg.cols.len(), self.cols.len());
        self.cfg
            .cols
            .iter()
            .zip(self.cols.iter())
            .map(|((name, _typ, stats_fn), col)| {
                #[cfg(debug_assertions)]
                {
                    assert_eq!(_typ, col.typ());
                }
                (name.as_str(), stats_fn, col)
            })
    }

    /// Explodes this _non-optional_ struct column into its component fields.
    ///
    /// Panics if this struct is optional.
    pub fn as_ref(&self) -> ColumnsRef {
        assert_none!(self.validity);
        ColumnsRef {
            validity: (),
            cols: self
                .cols()
                .map(|(n, _s, c)| (n.into(), c.clone()))
                .collect(),
        }
    }

    /// Explodes this _optional_ struct column into its component fields and
    /// validity.
    ///
    /// If the column is actually non-option, succeeds and acts as if every
    /// value is a Some.
    pub fn as_opt_ref(&self) -> ColumnsRef<ValidityRef> {
        ColumnsRef {
            validity: ValidityRef(self.validity.clone()),
            cols: self
                .cols()
                .map(|(n, _s, c)| (n.into(), c.clone()))
                .collect(),
        }
    }

    pub(crate) fn stats(&self, validity: ValidityRef) -> Result<OptionStats<StructStats>, String> {
        let validity = match (validity.0.as_ref(), self.validity.as_ref()) {
            (Some(_), Some(y)) => {
                debug_assert!(validity.is_superset(Some(y)));
                Some(y)
            }
            (Some(x), None) => Some(x),
            (None, Some(y)) => Some(y),
            (None, None) => None,
        };
        let mut cols = BTreeMap::new();
        for (n, s, c) in self.cols() {
            let stats = match s {
                StatsFn::Default => c.stats_default(ValidityRef(validity.cloned())),
                StatsFn::Custom(x) => x(c, ValidityRef(validity.cloned()))?,
            };
            cols.insert(n.to_owned(), stats);
        }
        let none = self.validity.as_ref().map_or(0, |x| x.null_count());
        Ok(OptionStats {
            none,
            some: StructStats {
                len: self.len(),
                cols,
            },
        })
    }

    pub(crate) fn to_arrow_struct(&self) -> Option<StructArray> {
        let (mut fields, mut arrays) = (Vec::new(), Vec::new());
        for (name, _stats_fn, col) in self.cols() {
            let (array, is_nullable) = col.to_arrow();
            fields.push(Field::new(name, array.data_type().clone(), is_nullable));
            arrays.push(array);
        }
        if fields.is_empty() {
            return None;
        }
        Some(StructArray::new(
            Fields::from(fields),
            arrays,
            self.validity.clone(),
        ))
    }

    /// Create a [`DynStructCol`] from an [`arrow::array::Array`].
    pub fn from_arrow(cfg: DynStructCfg, array: &dyn Array) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| format!("expected StructArray but was {:?}", array.data_type()))?;
        let len = array.len();
        let validity = array.logical_nulls();
        let mut cols = Vec::new();
        assert_eq!(cfg.cols.len(), array.num_columns());

        for ((_name, typ, _stats_fn), array) in cfg.cols.iter().zip(array.columns()) {
            let col = DynColumnRef::from_arrow(typ, array)?;
            cols.push(col);
        }
        Ok(DynStructCol {
            len,
            cfg,
            validity,
            cols,
        })
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if let Some(validity) = self.validity.as_ref() {
            if self.len != validity.len() {
                return Err(format!(
                    "validity len {} didn't match struct len {}",
                    validity.len(),
                    self.len()
                ));
            }
        }
        for (name, _stats, col) in self.cols() {
            if self.len != col.len() {
                return Err(format!(
                    "col {} len {} didn't match struct len {}",
                    name,
                    col.len(),
                    self.len
                ));
            }
        }
        Ok(())
    }
}

/// A [crate::columnar::ColumnPush] impl for [DynStruct].
#[derive(Debug)]
pub struct DynStructMut {
    cfg: DynStructCfg,
    len: usize,
    validity: Option<<bool as Data>::Mut>,
    cols: Vec<DynColumnMut>,
}

impl ColumnMut<DynStructCfg> for DynStructMut {
    fn new(cfg: &DynStructCfg) -> Self {
        let cfg = cfg.clone();
        let validity = None;
        let cols = cfg
            .cols
            .iter()
            .map(|(_name, typ, _stats_fn)| DynColumnMut::new_untyped(typ))
            .collect();
        DynStructMut {
            cfg,
            len: 0,
            validity,
            cols,
        }
    }

    fn cfg(&self) -> &DynStructCfg {
        &self.cfg
    }
}

impl ColumnPush<DynStruct> for DynStructMut {
    fn push<'a>(&mut self, val: DynStructRef<'a>) {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsMut, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        assert_none!(self.validity);
        self.push_cols(val);
    }

    fn finish(self) -> <DynStruct as Data>::Col {
        DynStructCol::from(self)
    }
}

impl ColumnPush<Option<DynStruct>> for DynStructMut {
    fn push<'a>(&mut self, val: Option<DynStructRef<'a>>) {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsMut, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        let len = self.len;
        let validity = self
            .validity
            // `BooleanBuilder` uses 1024 for its default capacity.
            .get_or_insert_with(|| BooleanBufferBuilder::new(1024));
        debug_assert_eq!(len, validity.len());

        if let Some(val) = val {
            validity.push(true);
            self.push_cols(val);
        } else {
            validity.push(false);
            self.push_cols(DynStructRef::Default);
        }
    }

    fn finish(self) -> <Option<DynStruct> as Data>::Col {
        DynStructCol::from(self)
    }
}

impl DynStructMut {
    /// Create a [`DynStructMut`] from individual parts.
    ///
    /// Note: it's up to the user to ensure the provided `cfg` has the same column types as the
    /// provided `cols`. We can't validate it here because [`DataType`]s are not easily comparable.
    pub fn from_parts(
        cfg: DynStructCfg,
        len: usize,
        validity: Option<BooleanBufferBuilder>,
        cols: Vec<DynColumnMut>,
    ) -> Self {
        DynStructMut {
            cfg,
            len,
            validity,
            cols,
        }
    }

    /// Returns the number of elements in this column
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the configuration for this column.
    pub fn cfg(&self) -> &DynStructCfg {
        &self.cfg
    }

    /// Explodes this _non-optional_ struct column into its component fields.
    ///
    /// Panics if this struct is optional.
    pub fn as_mut(self) -> ColumnsMut {
        let ColumnsMut {
            len,
            validity,
            cols,
        } = self.as_opt_mut();
        assert_none!(validity.validity);
        ColumnsMut {
            len,
            validity: (),
            cols,
        }
    }

    /// Explodes this _optional_ struct column into its component fields and
    /// validity.
    ///
    /// If the column is actually non-option, succeeds and acts as if every
    /// value is a Some.
    pub fn as_opt_mut(self) -> ColumnsMut<ValidityMut> {
        debug_assert_eq!(self.cfg.cols.len(), self.cols.len());
        let cols = self
            .cfg
            .cols
            .iter()
            .zip(self.cols)
            .map(|((name, _typ, _stats_fn), col)| {
                #[cfg(debug_assertions)]
                {
                    assert_eq!(_typ, col.typ());
                }
                (name.as_str().into(), col)
            })
            .collect();
        ColumnsMut {
            len: self.len,
            validity: ValidityMut {
                len: 0,
                validity: self.validity,
            },
            cols,
        }
    }

    fn push_cols(&mut self, val: DynStructRef<'_>) {
        match val {
            DynStructRef::Default => {
                for col in self.cols.iter_mut() {
                    col.push_default()
                }
            }
            DynStructRef::Idx { idx, cols } => {
                assert_eq!(cols.len(), self.cols.len());
                for (src, dst) in cols.iter().zip(self.cols.iter_mut()) {
                    dst.push_from(src, idx);
                }
            }
        }
        self.len += 1;
    }
}

impl From<DynStructMut> for DynStructCol {
    fn from(value: DynStructMut) -> Self {
        let cfg = value.cfg;
        let validity = value.validity.map(|b| NullBuffer::from(b.finish()));
        let cols = value
            .cols
            .into_iter()
            .map(DynColumnRef::from)
            .collect::<Vec<_>>();

        DynStructCol {
            len: value.len,
            cfg,
            validity,
            cols,
        }
    }
}

/// A new-type wrapper for an [`arrow`] validity column.
///
/// The [`arrow`] crate has an optimization where the validity column can be
/// elided if every value is true. This is the common case for the `Ok` struct
/// of our `SourceData`, so seems worth opting in to ourselves.
///
/// Note: [`NullBuffer`] is internally reference counted so cloning is cheap.
#[derive(Debug, Clone)]
pub struct ValidityRef(pub(crate) Option<NullBuffer>);

impl ValidityRef {
    /// Returns a validity that indicates true for all values.
    pub fn none() -> Self {
        ValidityRef(None)
    }

    /// Returns whether a column of optional structs is Some at the given index.
    /// If this is false, the contents of the struct's component fields at `idx`
    /// will be undefined.
    pub fn get(&self, idx: usize) -> bool {
        self.0.as_ref().map_or(true, |nulls| nulls.is_valid(idx))
    }

    /// Returns whether the set of all indexes that return `true` in `self` is a
    /// superset of the set of all indexes that return `true` in `other`.
    #[allow(clippy::bool_comparison)]
    pub fn is_superset(&self, other: Option<&NullBuffer>) -> bool {
        match (self.0.as_ref(), other) {
            (None, _) => {
                // None means all-true, which is trivially a superset.
                true
            }
            (Some(s), None) => {
                // None means all-true, so s is only a superset if it's entirely
                // true.
                s.null_count() == 0
            }
            (Some(s), Some(o)) => {
                assert_eq!(s.len(), o.len());
                for idx in 0..s.len() {
                    if s.is_valid(idx) == false && o.is_valid(idx) == true {
                        return false;
                    }
                }
                true
            }
        }
    }
}

/// A new-type wrapper for a mutable [`arrow`] validity column.
///
/// The [`arrow`] crate has an optimization where the validity column can be
/// elided if every value is true. This is the common case for the `Ok` struct
/// of our `SourceData`, so seems worth opting in to ourselves.
#[derive(Debug)]
pub struct ValidityMut {
    len: usize,
    validity: Option<BooleanBufferBuilder>,
}

impl ValidityMut {
    /// Pushes whether a column of optional structs is Some at the given index.
    /// If this is false, the contents of the struct's component fields at `idx`
    /// can be anything.
    pub fn push(&mut self, valid: bool) {
        if valid {
            if let Some(validity) = &mut self.validity {
                validity.push(valid);
            }
        } else {
            let validity = self.validity.get_or_insert_with(|| {
                let mut ret = BooleanBufferBuilder::new(self.len);
                ret.append_n(self.len, true);
                ret
            });
            validity.push(valid);
        }
        self.len += 1;
    }

    /// Consumes `self` returning the inner parts.
    pub fn into_parts(self) -> (usize, Option<BooleanBufferBuilder>) {
        (self.len, self.validity)
    }
}

/// A set of shared references to named columns.
///
/// This type implements a "builder"-esque pattern to help
/// [crate::columnar::Schema::decoder] impls. All columns should be removed via
/// [Self::col] and the [Self::finish] called to verify that all columns have
/// been accounted for.
#[derive(Debug)]
pub struct ColumnsRef<V = ()> {
    validity: V,
    pub(crate) cols: BTreeMap<Arc<str>, DynColumnRef>,
}

impl<V> ColumnsRef<V> {
    /// Removes the named typed column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<Arc<T::Col>, String> {
        self.dyn_col(name)?.downcast::<T>()
    }

    /// Removes the named dynamic column from the set.
    fn dyn_col(&mut self, name: &str) -> Result<DynColumnRef, String> {
        self.cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<V, String> {
        if self.cols.is_empty() {
            Ok(self.validity)
        } else {
            let names = self
                .cols
                .iter()
                .map(|(x, _)| x.to_string())
                .collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}

/// A set of exclusive references to named columns.
///
/// This type implements a "builder"-esque pattern to help
/// [crate::columnar::Schema::encoder] impls. All columns should be removed via
/// [Self::col] and the [Self::finish] called to verify that all columns have
/// been accounted for.
#[derive(Debug)]
pub struct ColumnsMut<V = ()> {
    len: usize,
    validity: V,
    pub(crate) cols: BTreeMap<Box<str>, DynColumnMut>,
}

impl<V> ColumnsMut<V> {
    /// Removes the named column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<Box<T::Mut>, String> {
        let col = self
            .cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))?;
        col.downcast::<T>()
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<(usize, V), String> {
        if self.cols.is_empty() {
            Ok((self.len, self.validity))
        } else {
            let names = self
                .cols
                .iter()
                .map(|(x, _)| x.as_ref())
                .collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}
