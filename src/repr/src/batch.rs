// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columns of datums laid out for evaluating many rows at once.
//!
//! A [`DatumBatch`] holds one column of a run of rows. Present values, nulls,
//! and errors live in separate lists: `values` holds the present values in row
//! order, `errors` holds the errors in row order, and `kinds` says for each
//! row which list it came from. A batch with neither nulls nor errors omits
//! `kinds`, so in the common case a batch is a bare [`TypedVec`] that a kernel
//! consumes as a slice.
//!
//! Evaluating a function over batches has three steps, see
//! [`DatumBatch::align`]: withhold the rows the function must not see (errors,
//! and nulls unless the function accepts them), run the function over the
//! remaining rows as typed values, and expand the result back over every row.

use std::borrow::Cow;

use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::{Datum, ReprScalarType};

/// Which list a row of a [`DatumBatch`] is stored in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// A present value, stored in the batch's values.
    Value,
    /// A null, stored nowhere.
    Null,
    /// An error, stored in the batch's errors.
    Error,
}

/// Strings stored as one byte buffer plus the end offset of each string.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Strings {
    ends: Vec<usize>,
    bytes: Vec<u8>,
}

impl Strings {
    /// The number of strings.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether there are no strings.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// Appends a string.
    pub fn push(&mut self, value: &str) {
        self.bytes.extend_from_slice(value.as_bytes());
        self.ends.push(self.bytes.len());
    }

    /// The string at `index`.
    pub fn get(&self, index: usize) -> &str {
        let start = index.checked_sub(1).map_or(0, |prev| self.ends[prev]);
        // Only whole `str`s are pushed, so each range holds valid UTF-8. The
        // check re-scans the bytes; a validated constructor could avoid it.
        std::str::from_utf8(&self.bytes[start..self.ends[index]]).expect("pushed from str")
    }

    /// The strings in order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        (0..self.len()).map(|index| self.get(index))
    }
}

/// A vector of values of one datum type.
#[derive(Clone, Debug, PartialEq)]
pub enum TypedVec {
    Bool(Vec<bool>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Strings),
}

/// The operations a [`TypedVec`] needs from each of its variants' containers.
trait Inner: Sized {
    fn gather(&self, indexes: &[usize]) -> Self;
    fn empty() -> Self;
}

impl<T: Copy> Inner for Vec<T> {
    fn gather(&self, indexes: &[usize]) -> Self {
        indexes.iter().map(|&index| self[index]).collect()
    }
    fn empty() -> Self {
        Vec::new()
    }
}

impl Inner for Strings {
    fn gather(&self, indexes: &[usize]) -> Self {
        let mut strings = Strings::default();
        for &index in indexes {
            strings.push(self.get(index));
        }
        strings
    }
    fn empty() -> Self {
        Strings::default()
    }
}

/// Applies `$body` to the container inside a `TypedVec`, whatever its variant.
macro_rules! with_inner {
    ($vec:expr, |$inner:ident| $body:expr) => {
        match $vec {
            TypedVec::Bool($inner) => $body,
            TypedVec::Int16($inner) => $body,
            TypedVec::Int32($inner) => $body,
            TypedVec::Int64($inner) => $body,
            TypedVec::UInt16($inner) => $body,
            TypedVec::UInt32($inner) => $body,
            TypedVec::UInt64($inner) => $body,
            TypedVec::Float32($inner) => $body,
            TypedVec::Float64($inner) => $body,
            TypedVec::String($inner) => $body,
        }
    };
}

/// Rebuilds a `TypedVec` of the same variant from `$body` applied to its container.
macro_rules! map_inner {
    ($vec:expr, |$inner:ident| $body:expr) => {
        match $vec {
            TypedVec::Bool($inner) => TypedVec::Bool($body),
            TypedVec::Int16($inner) => TypedVec::Int16($body),
            TypedVec::Int32($inner) => TypedVec::Int32($body),
            TypedVec::Int64($inner) => TypedVec::Int64($body),
            TypedVec::UInt16($inner) => TypedVec::UInt16($body),
            TypedVec::UInt32($inner) => TypedVec::UInt32($body),
            TypedVec::UInt64($inner) => TypedVec::UInt64($body),
            TypedVec::Float32($inner) => TypedVec::Float32($body),
            TypedVec::Float64($inner) => TypedVec::Float64($body),
            TypedVec::String($inner) => TypedVec::String($body),
        }
    };
}

impl TypedVec {
    /// An empty vector for values of `typ`, or `None` when `typ` has no typed vector.
    pub fn for_type(typ: &ReprScalarType) -> Option<Self> {
        Some(match typ {
            ReprScalarType::Bool => TypedVec::Bool(Vec::new()),
            ReprScalarType::Int16 => TypedVec::Int16(Vec::new()),
            ReprScalarType::Int32 => TypedVec::Int32(Vec::new()),
            ReprScalarType::Int64 => TypedVec::Int64(Vec::new()),
            ReprScalarType::UInt16 => TypedVec::UInt16(Vec::new()),
            ReprScalarType::UInt32 => TypedVec::UInt32(Vec::new()),
            ReprScalarType::UInt64 => TypedVec::UInt64(Vec::new()),
            ReprScalarType::Float32 => TypedVec::Float32(Vec::new()),
            ReprScalarType::Float64 => TypedVec::Float64(Vec::new()),
            ReprScalarType::String => TypedVec::String(Strings::default()),
            _ => return None,
        })
    }

    /// An empty vector of the same type as `self`.
    pub fn empty_like(&self) -> Self {
        map_inner!(self, |_inner| Inner::empty())
    }

    /// The number of values.
    pub fn len(&self) -> usize {
        with_inner!(self, |inner| inner.len())
    }

    /// Whether there are no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The values at `indexes`, in that order.
    pub fn gather(&self, indexes: &[usize]) -> Self {
        map_inner!(self, |inner| inner.gather(indexes))
    }

    /// The value at `index` as a datum.
    pub fn get(&self, index: usize) -> Datum<'_> {
        match self {
            TypedVec::Bool(v) => {
                if v[index] {
                    Datum::True
                } else {
                    Datum::False
                }
            }
            TypedVec::Int16(v) => Datum::Int16(v[index]),
            TypedVec::Int32(v) => Datum::Int32(v[index]),
            TypedVec::Int64(v) => Datum::Int64(v[index]),
            TypedVec::UInt16(v) => Datum::UInt16(v[index]),
            TypedVec::UInt32(v) => Datum::UInt32(v[index]),
            TypedVec::UInt64(v) => Datum::UInt64(v[index]),
            TypedVec::Float32(v) => Datum::Float32(OrderedFloat(v[index])),
            TypedVec::Float64(v) => Datum::Float64(OrderedFloat(v[index])),
            TypedVec::String(v) => Datum::String(v.get(index)),
        }
    }

    /// Appends `datum`, or returns `false` if it is not a non-null value of this vector's type.
    pub fn push_datum(&mut self, datum: Datum<'_>) -> bool {
        match (self, datum) {
            (TypedVec::Bool(v), Datum::True) => v.push(true),
            (TypedVec::Bool(v), Datum::False) => v.push(false),
            (TypedVec::Int16(v), Datum::Int16(x)) => v.push(x),
            (TypedVec::Int32(v), Datum::Int32(x)) => v.push(x),
            (TypedVec::Int64(v), Datum::Int64(x)) => v.push(x),
            (TypedVec::UInt16(v), Datum::UInt16(x)) => v.push(x),
            (TypedVec::UInt32(v), Datum::UInt32(x)) => v.push(x),
            (TypedVec::UInt64(v), Datum::UInt64(x)) => v.push(x),
            (TypedVec::Float32(v), Datum::Float32(x)) => v.push(x.into_inner()),
            (TypedVec::Float64(v), Datum::Float64(x)) => v.push(x.into_inner()),
            (TypedVec::String(v), Datum::String(x)) => v.push(x),
            _ => return false,
        }
        true
    }
}

/// Rust types with a [`TypedVec`] variant.
///
/// The lifetime is that of the vector a borrowed value such as `&str` reads from.
pub trait BatchValue<'a>: Sized {
    /// An empty vector for this type.
    fn empty() -> TypedVec;
    /// The values in `vec`, or `None` when `vec` holds another type.
    fn iter(vec: &'a TypedVec) -> Option<impl Iterator<Item = Self> + 'a>;
    /// Appends `self` to `vec`, which must hold this type.
    fn push(self, vec: &mut TypedVec);
}

macro_rules! impl_batch_value {
    ($native:ty, $variant:ident) => {
        impl<'a> BatchValue<'a> for $native {
            fn empty() -> TypedVec {
                TypedVec::$variant(Vec::new())
            }
            fn iter(vec: &'a TypedVec) -> Option<impl Iterator<Item = Self> + 'a> {
                match vec {
                    TypedVec::$variant(v) => Some(v.iter().copied()),
                    _ => None,
                }
            }
            fn push(self, vec: &mut TypedVec) {
                match vec {
                    TypedVec::$variant(v) => v.push(self),
                    _ => panic!(
                        "pushed a {} into a vector of another type",
                        std::any::type_name::<Self>()
                    ),
                }
            }
        }
    };
}

impl_batch_value!(bool, Bool);
impl_batch_value!(i16, Int16);
impl_batch_value!(i32, Int32);
impl_batch_value!(i64, Int64);
impl_batch_value!(u16, UInt16);
impl_batch_value!(u32, UInt32);
impl_batch_value!(u64, UInt64);
impl_batch_value!(f32, Float32);
impl_batch_value!(f64, Float64);

impl<'a> BatchValue<'a> for &'a str {
    fn empty() -> TypedVec {
        TypedVec::String(Strings::default())
    }
    fn iter(vec: &'a TypedVec) -> Option<impl Iterator<Item = Self> + 'a> {
        match vec {
            TypedVec::String(v) => Some(v.iter()),
            _ => None,
        }
    }
    fn push(self, vec: &mut TypedVec) {
        match vec {
            TypedVec::String(v) => v.push(self),
            _ => panic!("pushed a str into a vector of another type"),
        }
    }
}

/// One column of a run of rows: present values, nulls, and errors in separate lists.
#[derive(Clone, Debug)]
pub struct DatumBatch<E> {
    values: TypedVec,
    /// The list each row is stored in, or `None` when every row is a value.
    kinds: Option<Vec<Kind>>,
    errors: Vec<E>,
}

impl<E> DatumBatch<E> {
    /// A batch of the rows of `values`, none of them null or an error.
    pub fn new(values: TypedVec) -> Self {
        Self {
            values,
            kinds: None,
            errors: Vec::new(),
        }
    }

    /// Assembles a batch from its lists.
    ///
    /// `kinds` must hold one `Value` per element of `values` and one `Error`
    /// per element of `errors`. Without `kinds`, `errors` must be empty.
    pub fn from_parts(values: TypedVec, kinds: Option<Vec<Kind>>, errors: Vec<E>) -> Self {
        // A kinds list of only values says nothing; drop it so that
        // `is_dense` holds exactly when every row is a value.
        let kinds = kinds.filter(|kinds| kinds.iter().any(|kind| *kind != Kind::Value));
        match &kinds {
            Some(kinds) => {
                debug_assert_eq!(
                    kinds.iter().filter(|k| **k == Kind::Value).count(),
                    values.len()
                );
                debug_assert_eq!(
                    kinds.iter().filter(|k| **k == Kind::Error).count(),
                    errors.len()
                );
            }
            None => debug_assert!(errors.is_empty()),
        }
        Self {
            values,
            kinds,
            errors,
        }
    }

    /// `len` copies of `datum`, which must be null or a value of `typ`.
    ///
    /// `None` when `typ` has no typed vector or `datum` is not of that type.
    pub fn repeat(datum: Datum<'_>, len: usize, typ: &ReprScalarType) -> Option<Self> {
        let mut values = TypedVec::for_type(typ)?;
        if datum.is_null() {
            return Some(Self::from_parts(
                values,
                Some(vec![Kind::Null; len]),
                Vec::new(),
            ));
        }
        for _ in 0..len {
            if !values.push_datum(datum) {
                return None;
            }
        }
        Some(Self::new(values))
    }

    /// `len` copies of `error`, in a column of type `typ`.
    pub fn repeat_error(error: E, len: usize, typ: &ReprScalarType) -> Option<Self>
    where
        E: Clone,
    {
        let values = TypedVec::for_type(typ)?;
        Some(Self::from_parts(
            values,
            Some(vec![Kind::Error; len]),
            vec![error; len],
        ))
    }

    /// The number of rows.
    pub fn len(&self) -> usize {
        match &self.kinds {
            Some(kinds) => kinds.len(),
            None => self.values.len(),
        }
    }

    /// Whether there are no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The present values, in row order.
    pub fn values(&self) -> &TypedVec {
        &self.values
    }

    /// The errors, in row order.
    pub fn errors(&self) -> &[E] {
        &self.errors
    }

    /// Whether every row is a present value.
    pub fn is_dense(&self) -> bool {
        self.kinds.is_none()
    }

    /// The list `row` is stored in.
    pub fn kind(&self, row: usize) -> Kind {
        match &self.kinds {
            Some(kinds) => kinds[row],
            None => Kind::Value,
        }
    }

    fn kinds_mut(&mut self) -> &mut Vec<Kind> {
        // Every row pushed while `kinds` was absent was a value.
        let len = self.values.len();
        self.kinds.get_or_insert_with(|| vec![Kind::Value; len])
    }

    /// Appends a present value.
    pub fn push_value<'a, T: BatchValue<'a>>(&mut self, value: T) {
        value.push(&mut self.values);
        if let Some(kinds) = &mut self.kinds {
            kinds.push(Kind::Value);
        }
    }

    /// Appends a null.
    pub fn push_null(&mut self) {
        self.kinds_mut().push(Kind::Null);
    }

    /// Appends an error.
    pub fn push_error(&mut self, error: E) {
        self.kinds_mut().push(Kind::Error);
        self.errors.push(error);
    }

    /// Appends `datum`, or returns `false` if it is neither null nor a value of this batch's type.
    pub fn push_datum(&mut self, datum: Datum<'_>) -> bool {
        if datum.is_null() {
            self.push_null();
            return true;
        }
        if !self.values.push_datum(datum) {
            return false;
        }
        if let Some(kinds) = &mut self.kinds {
            kinds.push(Kind::Value);
        }
        true
    }

    /// The rows in order, each a datum (null for a null row) or an error.
    pub fn iter(&self) -> impl Iterator<Item = Result<Datum<'_>, &E>> {
        let (mut value, mut error) = (0, 0);
        (0..self.len()).map(move |row| match self.kind(row) {
            Kind::Value => {
                let datum = self.values.get(value);
                value += 1;
                Ok(datum)
            }
            Kind::Null => Ok(Datum::Null),
            Kind::Error => {
                let err = &self.errors[error];
                error += 1;
                Err(err)
            }
        })
    }

    /// The error rows, as `(row, error)`.
    pub fn iter_errors(&self) -> impl Iterator<Item = (usize, &E)> {
        let kinds = self.kinds.as_deref().unwrap_or(&[]);
        kinds
            .iter()
            .enumerate()
            .filter(|(_, kind)| **kind == Kind::Error)
            .map(|(row, _)| row)
            .zip_eq(self.errors.iter())
    }

    /// Prepares `inputs`, one per function argument and all of the same
    /// length, for a batch kernel.
    ///
    /// A row holding an error in any input is withheld from the kernel and
    /// carries the first such error in argument order. A row holding a null in
    /// any input is withheld as null unless `pass_nulls`, in which case the
    /// kernel sees it. This matches row-at-a-time evaluation, where an error
    /// argument wins over a null one and a null argument to a function that
    /// does not accept nulls yields null without calling it.
    pub fn align<'a>(inputs: &[&'a Self], pass_nulls: bool) -> Aligned<'a, E>
    where
        E: Clone,
    {
        let withhold = |kind: Kind| match kind {
            Kind::Value => false,
            Kind::Null => !pass_nulls,
            Kind::Error => true,
        };
        let nothing_withheld = inputs.iter().all(|input| match &input.kinds {
            None => true,
            Some(kinds) => !kinds.iter().any(|kind| withhold(*kind)),
        });
        if nothing_withheld {
            return Aligned {
                inputs: inputs.iter().map(|input| Cow::Borrowed(*input)).collect(),
                withheld: None,
            };
        }

        let len = inputs[0].len();
        debug_assert!(inputs.iter().all(|input| input.len() == len));
        let mut kinds = Vec::with_capacity(len);
        let mut errors = Vec::new();
        // Per input: the indexes into its values to keep, the kinds of its
        // kept rows, and cursors into its values and errors.
        let mut keep = vec![Vec::with_capacity(len); inputs.len()];
        let mut kept_kinds = vec![Vec::with_capacity(len); inputs.len()];
        let mut value_cursor = vec![0; inputs.len()];
        let mut error_cursor = vec![0; inputs.len()];
        for row in 0..len {
            let mut fate = Kind::Value;
            let mut first_error = None;
            for (i, input) in inputs.iter().enumerate() {
                match input.kind(row) {
                    Kind::Value => {}
                    Kind::Null => {
                        if !pass_nulls && fate == Kind::Value {
                            fate = Kind::Null;
                        }
                    }
                    Kind::Error => {
                        if first_error.is_none() {
                            first_error = Some(input.errors[error_cursor[i]].clone());
                        }
                        fate = Kind::Error;
                    }
                }
            }
            for (i, input) in inputs.iter().enumerate() {
                let kind = input.kind(row);
                if fate == Kind::Value {
                    if kind == Kind::Value {
                        keep[i].push(value_cursor[i]);
                    }
                    kept_kinds[i].push(kind);
                }
                match kind {
                    Kind::Value => value_cursor[i] += 1,
                    Kind::Error => error_cursor[i] += 1,
                    Kind::Null => {}
                }
            }
            kinds.push(fate);
            errors.extend(first_error);
        }

        let inputs = inputs
            .iter()
            .zip_eq(keep)
            .zip_eq(kept_kinds)
            .map(|((input, keep), kept_kinds)| {
                if kept_kinds.len() == input.len() {
                    // No row of this input was withheld.
                    Cow::Borrowed(*input)
                } else {
                    let kinds = kept_kinds.contains(&Kind::Null).then_some(kept_kinds);
                    Cow::Owned(Self::from_parts(
                        input.values.gather(&keep),
                        kinds,
                        Vec::new(),
                    ))
                }
            })
            .collect();
        Aligned {
            inputs,
            withheld: Some((kinds, errors)),
        }
    }
}

/// Kernel inputs prepared by [`DatumBatch::align`], with the rows withheld from the kernel.
#[derive(Debug)]
pub struct Aligned<'a, E: Clone> {
    inputs: Vec<Cow<'a, DatumBatch<E>>>,
    /// Every row's kind and the withheld errors, or `None` when no row was
    /// withheld. `Value` marks the rows the kernel sees.
    withheld: Option<(Vec<Kind>, Vec<E>)>,
}

impl<'a, E: Clone> Aligned<'a, E> {
    /// The inputs the kernel sees.
    pub fn inputs(&self) -> Vec<&DatumBatch<E>> {
        self.inputs.iter().map(|input| &**input).collect()
    }

    /// Expands `output`, one row per row the kernel saw, back over every row.
    pub fn finish(self, output: DatumBatch<E>) -> DatumBatch<E> {
        let Some((kinds, withheld_errors)) = self.withheld else {
            return output;
        };
        debug_assert_eq!(
            kinds.iter().filter(|kind| **kind == Kind::Value).count(),
            output.len()
        );
        let DatumBatch {
            values,
            kinds: output_kinds,
            errors: output_errors,
        } = output;
        let mut output_kinds = output_kinds.map(Vec::into_iter);
        let mut output_errors = output_errors.into_iter();
        let mut withheld_errors = withheld_errors.into_iter();
        let mut errors = Vec::new();
        let kinds = kinds
            .into_iter()
            .map(|kind| match kind {
                Kind::Value => {
                    let kind = match &mut output_kinds {
                        Some(output_kinds) => output_kinds.next().expect("one kind per kernel row"),
                        None => Kind::Value,
                    };
                    if kind == Kind::Error {
                        errors.push(output_errors.next().expect("one error per error row"));
                    }
                    kind
                }
                Kind::Null => Kind::Null,
                Kind::Error => {
                    errors.push(
                        withheld_errors
                            .next()
                            .expect("one error per withheld error row"),
                    );
                    Kind::Error
                }
            })
            .collect();
        DatumBatch::from_parts(values, Some(kinds), errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int64s(rows: &[Result<Option<i64>, &str>]) -> DatumBatch<String> {
        let mut batch = DatumBatch::new(TypedVec::Int64(Vec::new()));
        for row in rows {
            match row {
                Ok(Some(v)) => batch.push_value(*v),
                Ok(None) => batch.push_null(),
                Err(e) => batch.push_error(e.to_string()),
            }
        }
        batch
    }

    fn rows(batch: &DatumBatch<String>) -> Vec<Result<Option<i64>, String>> {
        batch
            .iter()
            .map(|row| match row {
                Ok(Datum::Null) => Ok(None),
                Ok(Datum::Int64(v)) => Ok(Some(v)),
                Ok(other) => panic!("unexpected datum {other:?}"),
                Err(e) => Err(e.clone()),
            })
            .collect()
    }

    #[mz_ore::test]
    fn dense_batches_stay_dense() {
        let batch = int64s(&[Ok(Some(1)), Ok(Some(2))]);
        assert!(batch.is_dense());
        assert_eq!(batch.len(), 2);
        assert_eq!(rows(&batch), vec![Ok(Some(1)), Ok(Some(2))]);
    }

    #[mz_ore::test]
    fn kinds_appear_on_first_null_or_error() {
        let batch = int64s(&[Ok(Some(1)), Ok(None), Err("boom"), Ok(Some(4))]);
        assert!(!batch.is_dense());
        assert_eq!(batch.values(), &TypedVec::Int64(vec![1, 4]));
        assert_eq!(batch.errors(), &["boom".to_string()]);
        assert_eq!(
            rows(&batch),
            vec![Ok(Some(1)), Ok(None), Err("boom".to_string()), Ok(Some(4))]
        );
        assert_eq!(
            batch.iter_errors().collect::<Vec<_>>(),
            vec![(2, &"boom".to_string())]
        );
    }

    #[mz_ore::test]
    fn align_withholds_errors_and_nulls() {
        let a = int64s(&[Ok(Some(1)), Ok(None), Err("a"), Ok(Some(4)), Ok(Some(5))]);
        let b = int64s(&[Ok(Some(10)), Ok(Some(20)), Err("b"), Ok(None), Ok(Some(50))]);
        let aligned = DatumBatch::align(&[&a, &b], false);
        let inputs = aligned.inputs();
        assert_eq!(inputs[0].values(), &TypedVec::Int64(vec![1, 5]));
        assert_eq!(inputs[1].values(), &TypedVec::Int64(vec![10, 50]));
        assert!(inputs[0].is_dense() && inputs[1].is_dense());

        // The kernel adds and fails on the last row.
        let mut output = DatumBatch::new(TypedVec::Int64(Vec::new()));
        output.push_value(11i64);
        output.push_error("overflow".to_string());
        let result = aligned.finish(output);
        assert_eq!(
            rows(&result),
            vec![
                Ok(Some(11)),
                Ok(None),
                Err("a".to_string()),
                Ok(None),
                Err("overflow".to_string()),
            ]
        );
    }

    #[mz_ore::test]
    fn align_passes_nulls_when_asked() {
        let a = int64s(&[Ok(Some(1)), Ok(None), Err("a")]);
        let aligned = DatumBatch::align(&[&a], true);
        let inputs = aligned.inputs();
        assert_eq!(inputs[0].len(), 2);
        assert_eq!(rows(inputs[0]), vec![Ok(Some(1)), Ok(None)]);

        let mut output = DatumBatch::new(TypedVec::Bool(Vec::new()));
        output.push_value(false);
        output.push_value(true);
        let result = aligned.finish(output);
        assert_eq!(result.kind(0), Kind::Value);
        assert_eq!(result.kind(1), Kind::Value);
        assert_eq!(result.kind(2), Kind::Error);
        assert_eq!(result.values(), &TypedVec::Bool(vec![false, true]));
    }

    #[mz_ore::test]
    fn align_borrows_when_nothing_is_withheld() {
        let a = int64s(&[Ok(Some(1)), Ok(Some(2))]);
        let aligned = DatumBatch::align(&[&a], false);
        assert!(matches!(aligned.inputs[0], Cow::Borrowed(_)));
        let output = int64s(&[Ok(Some(2)), Ok(Some(3))]);
        assert!(aligned.finish(output).is_dense());
    }

    #[mz_ore::test]
    fn repeat_and_strings() {
        let batch =
            DatumBatch::<String>::repeat(Datum::String("ab"), 3, &ReprScalarType::String).unwrap();
        assert_eq!(
            batch.iter().collect::<Vec<_>>(),
            vec![Ok(Datum::String("ab")); 3]
        );
        let nulls = DatumBatch::<String>::repeat(Datum::Null, 2, &ReprScalarType::Int64).unwrap();
        assert_eq!(nulls.iter().collect::<Vec<_>>(), vec![Ok(Datum::Null); 2]);
        assert!(DatumBatch::<String>::repeat(Datum::Int64(1), 2, &ReprScalarType::Jsonb).is_none());
        assert!(DatumBatch::<String>::repeat(Datum::Int32(1), 2, &ReprScalarType::Int64).is_none());
    }
}
