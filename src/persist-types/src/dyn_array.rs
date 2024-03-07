// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dynamic columnar array.

use std::sync::Arc;

use arrow2::array::growable::{Growable, GrowableList};
use arrow2::array::{Array, ListArray};
use arrow2::io::parquet::write::Encoding;
use arrow2::offset::OffsetsBuffer;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{ColumnCfg, ColumnFormat, ColumnGet, ColumnPush, Data, DataType};
use crate::dyn_col::{DynColumnMut, DynColumnRef};
use crate::stats::StatsFn;

/// A columnar array.
#[derive(Debug)]
pub struct DynArray;

/// Describes the inner type of a [`DynArray`] and the stats we collect for it.
#[derive(Debug, Clone)]
#[cfg_attr(debug_assertions, derive(PartialEq))]
pub struct DynArrayCfg {
    /// The inner type of this array.
    pub col: Arc<(DataType, StatsFn)>,
}

impl DynArrayCfg {
    /// Create a new [`DynArrayCfg`] where `ty` is the inner type of the array.
    ///
    /// For example, an array if `i32`s would pass a `ty` with [`ColumnFormat::I32`].
    pub fn new(ty: DataType, stats: StatsFn) -> Self {
        DynArrayCfg {
            col: Arc::new((ty, stats)),
        }
    }
}

impl ColumnCfg<DynArray> for DynArrayCfg {
    fn as_type(&self) -> DataType {
        DataType {
            optional: false,
            format: ColumnFormat::List(self.clone()),
        }
    }
}

/// The [Data::Ref] type for [`DynArray`].
#[derive(Debug, Default)]
pub enum DynArrayRef<'a> {
    /// An empty inner array.
    ///
    /// Note: This variant exists because we need to have some default value for [`DynArrayRef`]
    /// and [`DynColumnRef`] itself does not implement default.
    #[default]
    Empty,
    /// One instance of the inner array.
    Value(&'a DynColumnRef),
}

/// A [`ColumnGet`] impl for [`DynArray`].
///
/// [`ColumnGet`]: crate::columnar::ColumnGet
#[derive(Debug)]
pub struct DynArrayCol {
    len: usize,
    cfg: DynArrayCfg,
    pub(crate) validity: Option<<bool as Data>::Col>,
    pub(crate) arrays: Vec<DynColumnRef>,
}

impl ColumnRef<DynArrayCfg> for DynArrayCol {
    fn cfg(&self) -> &DynArrayCfg {
        &self.cfg
    }

    fn len(&self) -> usize {
        self.len
    }

    fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        let cols: Vec<ListArray<i64>> = self
            .arrays
            .iter()
            .map(|array| {
                let (_encoding, col, is_nullable) = array.to_arrow();
                let field =
                    arrow2::datatypes::Field::new("item", col.data_type().clone(), is_nullable);
                let data_type = arrow2::datatypes::DataType::LargeList(Box::new(field));
                ListArray::<i64>::new(data_type, OffsetsBuffer::new(), col, None)
            })
            .collect();
        let refs = cols.iter().collect();

        // Create the final ListArray, I think?
        let mut growable = GrowableList::new(refs, false, cols.len());
        for (idx, col) in cols.iter().enumerate() {
            growable.extend(idx, 0, col.len());
        }
        let array = ListArray::<i64>::from(growable);

        (Encoding::Plain, Box::new(array))
    }

    fn from_arrow(cfg: &DynArrayCfg, array: &Box<dyn Array>) -> Result<Self, String> {
        let col = array
            .as_any()
            .downcast_ref::<ListArray<i64>>()
            .ok_or_else(|| format!("expected ListArray<i64> but was {:?}", array.data_type()))?;
        let len = array.len();
        let validity = array.validity().cloned();
        let arrays = col
            .values_iter()
            .map(|array| DynColumnRef::from_arrow(&cfg.col.0, &array))
            .collect::<Result<Vec<_>, String>>()?;

        Ok(DynArrayCol {
            len,
            cfg: cfg.clone(),
            validity,
            arrays,
        })
    }
}

impl ColumnGet<DynArray> for DynArrayCol {
    fn get<'a>(&'a self, idx: usize) -> <DynArray as Data>::Ref<'a> {
        assert!(self.validity.is_none());
        let array = self.arrays.get(idx).expect("index to be valid");
        DynArrayRef::Value(array)
    }
}

impl ColumnMut<DynArrayCfg> for DynArrayCol {
    fn new(cfg: &DynArrayCfg) -> Self {
        DynArrayCol {
            len: 0,
            cfg: cfg.clone(),
            validity: None,
            arrays: Vec::new(),
        }
    }

    fn cfg(&self) -> &DynArrayCfg {
        &self.cfg
    }
}

impl ColumnPush<DynArray> for DynArrayCol {
    fn push<'a>(&mut self, val: DynArrayRef<'a>) {
        let array = match val {
            DynArrayRef::Empty => {
                let new = DynColumnMut::new_untyped(&self.cfg.col.0);
                DynColumnRef::from(new)
            }
            DynArrayRef::Value(array) => array.clone(),
        };
        self.arrays.push(array);
        self.len = self.len + 1;
    }
}
