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

use arrow2::array::{Array, NullArray, StructArray};
use arrow2::bitmap::MutableBitmap;
use arrow2::datatypes::{DataType as ArrowLogicalType, Field};
use arrow2::io::parquet::write::Encoding;

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
    len: usize,
    cfg: DynStructCfg,
    pub(crate) validity: Option<<bool as Data>::Col>,
    pub(crate) cols: Vec<DynColumnRef>,
}

impl ColumnRef<DynStructCfg> for DynStructCol {
    fn cfg(&self) -> &DynStructCfg {
        &self.cfg
    }
    fn len(&self) -> usize {
        self.len
    }
    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        let array: Box<dyn Array> = match self.to_arrow_struct() {
            Some((array, _col_encodings)) => Box::new(array),
            None => Box::new(NullArray::new_empty(ArrowLogicalType::Null)),
        };
        (Encoding::Plain, array)
    }
    fn from_arrow(cfg: &DynStructCfg, array: &Box<dyn Array>) -> Result<Self, String> {
        Self::from_arrow(cfg.clone(), array)
    }
}

impl ColumnGet<DynStruct> for DynStructCol {
    fn get<'a>(&'a self, idx: usize) -> DynStructRef<'a> {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsRef, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        assert!(self.validity.is_none());
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
        if self.validity.as_ref().map_or(true, |x| x.get_bit(idx)) {
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
    pub fn as_ref<'a>(&'a self) -> ColumnsRef<'a> {
        assert!(self.validity.is_none());
        ColumnsRef {
            validity: (),
            cols: self.cols().map(|(n, _s, c)| (n, c)).collect(),
        }
    }

    /// Explodes this _optional_ struct column into its component fields and
    /// validity.
    ///
    /// If the column is actually non-option, succeeds and acts as if every
    /// value is a Some.
    pub fn as_opt_ref<'a>(&'a self) -> ColumnsRef<'a, ValidityRef<'a>> {
        ColumnsRef {
            validity: ValidityRef(self.validity.as_ref()),
            cols: self.cols().map(|(n, _s, c)| (n, c)).collect(),
        }
    }

    pub(crate) fn stats(
        &self,
        validity: ValidityRef<'_>,
    ) -> Result<OptionStats<StructStats>, String> {
        let validity = match (validity.0, self.validity.as_ref()) {
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
                StatsFn::Default => c.stats_default(ValidityRef(validity)),
                StatsFn::Custom(x) => x(c, ValidityRef(validity))?,
            };
            cols.insert(n.to_owned(), stats);
        }
        let none = self.validity.as_ref().map_or(0, |x| x.unset_bits());
        Ok(OptionStats {
            none,
            some: StructStats {
                len: self.len(),
                cols,
            },
        })
    }

    pub(crate) fn to_arrow_struct(&self) -> Option<(StructArray, Vec<Encoding>)> {
        let (mut fields, mut encodings, mut arrays) = (Vec::new(), Vec::new(), Vec::new());
        for (name, _stats_fn, col) in self.cols() {
            let (encoding, array, is_nullable) = col.to_arrow();
            fields.push(Field::new(name, array.data_type().clone(), is_nullable));
            encodings.push(encoding);
            arrays.push(array);
        }
        if fields.is_empty() {
            return None;
        }
        let array = StructArray::new(ArrowLogicalType::Struct(fields), arrays, None);
        Some((array, encodings))
    }

    #[allow(clippy::borrowed_box)]
    pub(crate) fn from_arrow(cfg: DynStructCfg, array: &Box<dyn Array>) -> Result<Self, String> {
        let array = array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| format!("expected StructArray but was {:?}", array.data_type()))?;
        let validity = array.validity().cloned();
        let mut cols = Vec::new();
        assert_eq!(cfg.cols.len(), array.values().len());
        for ((_name, typ, _stats_fn), array) in cfg.cols.iter().zip(array.values()) {
            let col = DynColumnRef::from_arrow(typ, array)?;
            cols.push(col);
        }
        let len = cols.first().map_or(0, |x| x.len());
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
        assert!(self.validity.is_none());
        self.push_cols(val);
    }
}

impl ColumnPush<Option<DynStruct>> for DynStructMut {
    fn push<'a>(&mut self, val: Option<DynStructRef<'a>>) {
        // NB: For efficiency, production codepaths instead deconstruct the
        // columnar struct into components via ColumnsMut, so this is unused. We
        // keep it for completeness and also so that any future changes to the
        // code consider it.
        let len = self.len;
        if let Some(validity) = self.validity.as_ref() {
            debug_assert_eq!(len, validity.len());
        }
        let mut validity = ValidityMut {
            len,
            validity: &mut self.validity,
        };

        if let Some(val) = val {
            validity.push(true);
            self.push_cols(val);
        } else {
            validity.push(false);
            self.push_cols(DynStructRef::Default);
        }
    }
}

impl DynStructMut {
    /// Returns the number of elements in this column
    pub fn len(&self) -> usize {
        self.len
    }

    /// Explodes this _non-optional_ struct column into its component fields.
    ///
    /// Panics if this struct is optional.
    pub fn as_mut<'a>(&'a mut self) -> ColumnsMut<'a> {
        let ColumnsMut { validity, cols } = self.as_opt_mut();
        assert!(validity.validity.is_none());
        ColumnsMut { validity: (), cols }
    }

    /// Explodes this _optional_ struct column into its component fields and
    /// validity.
    ///
    /// If the column is actually non-option, succeeds and acts as if every
    /// value is a Some.
    pub fn as_opt_mut<'a>(&'a mut self) -> ColumnsMut<'a, ValidityMut<'a>> {
        debug_assert_eq!(self.cfg.cols.len(), self.cols.len());
        let cols = self
            .cfg
            .cols
            .iter()
            .zip(self.cols.iter_mut())
            .map(|((name, _typ, _stats_fn), col)| {
                #[cfg(debug_assertions)]
                {
                    assert_eq!(_typ, col.typ());
                }
                (name.as_str(), col)
            })
            .collect();
        ColumnsMut {
            validity: ValidityMut {
                len: 0,
                validity: &mut self.validity,
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
        let validity = value.validity.map(<bool as Data>::Col::from);
        let cols = value
            .cols
            .into_iter()
            .map(DynColumnRef::from)
            .collect::<Vec<_>>();
        let len = cols.first().map_or(0, |x| x.len());
        DynStructCol {
            len,
            cfg,
            validity,
            cols,
        }
    }
}

/// A new-type wrapper for an `arrow2` validity column.
///
/// The `arrow2` crate has an optimization where the validity column can be
/// elided if every value is true. This is the common case for the `Ok` struct
/// of our `SourceData`, so seems worth opting in to ourselves.
#[derive(Debug, Clone, Copy)]
pub struct ValidityRef<'a>(pub(crate) Option<&'a <bool as Data>::Col>);

impl ValidityRef<'_> {
    /// Returns a validity that indicates true for all values.
    pub fn none() -> Self {
        ValidityRef(None)
    }

    /// Returns whether a column of optional structs is Some at the given index.
    /// If this is false, the contents of the struct's component fields at `idx`
    /// will be undefined.
    pub fn get(&self, idx: usize) -> bool {
        self.0.map_or(true, |x| x.get_bit(idx))
    }

    /// Returns whether the set of all indexes that return `true` in `self` is a
    /// superset of the set of all indexes that return `true` in `other`.
    #[allow(clippy::bool_comparison)]
    pub fn is_superset(&self, other: Option<&<bool as Data>::Col>) -> bool {
        match (self.0, other) {
            (None, _) => {
                // None means all-true, which is trivially a superset.
                true
            }
            (Some(s), None) => {
                // None means all-true, so s is only a superset if it's entirely
                // true.
                s.unset_bits() == 0
            }
            (Some(s), Some(o)) => {
                assert_eq!(s.len(), o.len());
                for idx in 0..s.len() {
                    if s.get_bit(idx) == false && o.get_bit(idx) == true {
                        return false;
                    }
                }
                true
            }
        }
    }
}

/// A new-type wrapper for a mutable `arrow2` validity column.
///
/// The `arrow2` crate has an optimization where the validity column can be
/// elided if every value is true. This is the common case for the `Ok` struct
/// of our `SourceData`, so seems worth opting in to ourselves.
#[derive(Debug)]
pub struct ValidityMut<'a> {
    len: usize,
    validity: &'a mut Option<<bool as Data>::Mut>,
}

impl ValidityMut<'_> {
    /// Pushes whether a column of optional structs is Some at the given index.
    /// If this is false, the contents of the struct's component fields at `idx`
    /// can be anything.
    pub fn push(&mut self, valid: bool) {
        if valid {
            if let Some(validity) = self.validity {
                validity.push(valid);
            }
        } else {
            let validity = self.validity.get_or_insert_with(|| {
                let mut ret = MutableBitmap::new();
                ret.extend_constant(self.len, true);
                ret
            });
            validity.push(valid);
        }
        self.len += 1;
    }
}

/// A set of shared references to named columns.
///
/// This type implements a "builder"-esque pattern to help
/// [crate::columnar::Schema::decoder] impls. All columns should be removed via
/// [Self::col] and the [Self::finish] called to verify that all columns have
/// been accounted for.
#[derive(Debug)]
pub struct ColumnsRef<'a, V = ()> {
    validity: V,
    pub(crate) cols: BTreeMap<&'a str, &'a DynColumnRef>,
}

impl<'a, V> ColumnsRef<'a, V> {
    /// Removes the named typed column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a T::Col, String> {
        self.dyn_col(name)?.downcast::<T>()
    }

    /// Removes the named dynamic column from the set.
    fn dyn_col(&mut self, name: &str) -> Result<&'a DynColumnRef, String> {
        self.cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<V, String> {
        if self.cols.is_empty() {
            Ok(self.validity)
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
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
pub struct ColumnsMut<'a, V = ()> {
    validity: V,
    pub(crate) cols: BTreeMap<&'a str, &'a mut DynColumnMut>,
}

impl<'a, V> ColumnsMut<'a, V> {
    /// Removes the named column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a mut T::Mut, String> {
        let col = self
            .cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))?;
        col.downcast::<T>()
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<V, String> {
        if self.cols.is_empty() {
            Ok(self.validity)
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}
