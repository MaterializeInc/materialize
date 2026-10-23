// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The units of work the validator charges for an operator's own content.
//!
//! The rules spend work proportional to what an operator states: its scalar expressions,
//! keys, aggregates and plan structures, and the datums of a constant. [`content`] measures
//! that by counting the items of the operator's serialized form, skipping its inputs, which
//! are charged when they are visited.

use mz_repr::Row;
use serde::Serialize;
use serde::ser::{self, Serializer};

use crate::plan::LirRelationNode;

/// An upper bound, up to a constant factor, on the work the rules spend on `node`, excluding
/// its inputs. Computing it costs work proportional to its result.
pub(super) fn content(node: &LirRelationNode) -> usize {
    match node {
        // Serializing rows encodes them, so constants are measured directly. Each datum takes
        // at least one byte, so a row's byte length bounds its datums.
        LirRelationNode::Constant { rows } => match rows {
            Ok(rows) => rows
                .iter()
                .map(|(row, _, _)| 1 + Row::data_len(row))
                .fold(1, usize::saturating_add),
            Err(_) => 1,
        },
        _ => {
            let mut counter = Counter { items: 0 };
            // `Counter` never fails.
            let _ = node.serialize(&mut counter);
            counter.items
        }
    }
}

/// A serializer that counts the items it is given, and skips nested relation expressions.
struct Counter {
    items: usize,
}

impl Counter {
    fn add(&mut self, n: usize) {
        self.items = self.items.saturating_add(n);
    }
}

#[derive(Debug)]
struct Never;

impl std::fmt::Display for Never {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unreachable")
    }
}

impl std::error::Error for Never {}

impl ser::Error for Never {
    fn custom<T: std::fmt::Display>(_msg: T) -> Self {
        Never
    }
}

/// A compound value being counted. With `skip`, its fields are not visited.
struct Compound<'a> {
    counter: &'a mut Counter,
    skip: bool,
}

impl Compound<'_> {
    fn element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        if self.skip {
            return Ok(());
        }
        value.serialize(&mut *self.counter)
    }
}

impl<'a> Serializer for &'a mut Counter {
    type Ok = ();
    type Error = Never;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, _: bool) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_i8(self, _: i8) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_i16(self, _: i16) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_i32(self, _: i32) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_i64(self, _: i64) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_i128(self, _: i128) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_u8(self, _: u8) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_u16(self, _: u16) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_u32(self, _: u32) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_u64(self, _: u64) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_u128(self, _: u128) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_f32(self, _: f32) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_f64(self, _: f64) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_char(self, _: char) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<(), Never> {
        self.add(1 + v.len());
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Never> {
        self.add(1 + v.len());
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<(), Never> {
        self.add(1);
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Never> {
        self.add(1);
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Never> {
        self.add(1);
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        value: &T,
    ) -> Result<(), Never> {
        self.add(1);
        value.serialize(self)
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Compound<'a>, Never> {
        self.add(1);
        Ok(Compound {
            counter: self,
            skip: false,
        })
    }
    fn serialize_tuple(self, _: usize) -> Result<Compound<'a>, Never> {
        self.serialize_seq(None)
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, Never> {
        self.serialize_seq(None)
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Never> {
        self.serialize_seq(None)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Compound<'a>, Never> {
        self.serialize_seq(None)
    }
    fn serialize_struct(self, name: &'static str, _: usize) -> Result<Compound<'a>, Never> {
        self.add(1);
        // Inputs are charged when they are visited.
        let skip = name == "LirRelationExpr";
        Ok(Compound {
            counter: self,
            skip,
        })
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Never> {
        self.serialize_seq(None)
    }
}

impl ser::SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Never> {
        self.element(key)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Never;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _: &'static str,
        value: &T,
    ) -> Result<(), Never> {
        self.element(value)
    }
    fn end(self) -> Result<(), Never> {
        Ok(())
    }
}
