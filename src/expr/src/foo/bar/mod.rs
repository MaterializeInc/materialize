// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::NaiveDate;
use mz_repr::adt::char::CharLength;
use mz_repr::chrono::any_naive_date;
use mz_repr::proto::newapi::*;
use mz_repr::GlobalId;
use proptest::prelude::*;
use proptest_derive::Arbitrary;

include!(concat!(env!("OUT_DIR"), "/mz_expr.foo.bar.rs"));

// `$T` is a struct
#[derive(Arbitrary, Debug, PartialEq, Eq)]
pub struct MyStruct {
    pub field_1: u64,
    pub field_2: usize,
    pub field_3: CharLength,
    #[proptest(strategy = "any_naive_date()")]
    pub field_4: NaiveDate,
    #[proptest(strategy = "tiny_char_length_vec()")]
    pub field_5: Vec<CharLength>,
    #[proptest(strategy = "prop::collection::vec(tiny_char_length_vec(), 0..3)")]
    pub field_6: Vec<Vec<CharLength>>,
    #[proptest(strategy = "tiny_id_to_naive_date_map()")]
    pub field_7: HashMap<GlobalId, NaiveDate>,
}

fn tiny_char_length_vec() -> prop::strategy::BoxedStrategy<Vec<CharLength>> {
    prop::collection::vec(any::<CharLength>(), 0..3).boxed()
}

fn tiny_id_to_naive_date_map() -> prop::strategy::BoxedStrategy<HashMap<GlobalId, NaiveDate>> {
    prop::collection::hash_map(any::<GlobalId>(), any_naive_date(), 0..3).boxed()
}

// `$T` is an enum
#[derive(Arbitrary, Debug, PartialEq, Eq, Hash)]
pub enum MyEnum {
    Var1(u64),
    Var2(usize),
    Var3(CharLength),
    Var4(#[proptest(strategy = "any_naive_date()")] NaiveDate),
}

impl RustType<ProtoMyStruct> for MyStruct {
    fn into_proto(&self) -> ProtoMyStruct {
        ProtoMyStruct {
            field_1: self.field_1,
            field_2: self.field_2.into_proto(),
            field_3: Some(self.field_3.into_proto()),
            field_4: Some(self.field_4.into_proto()),
            field_5: self.field_5.into_proto(),
            field_6: self.field_6.into_proto(),
            field_7: self.field_7.into_proto(),
        }
    }

    fn from_proto(proto: ProtoMyStruct) -> Result<Self, TryFromProtoError> {
        Ok(MyStruct {
            field_1: proto.field_1,
            field_2: proto.field_2.into_rust()?,
            field_3: proto.field_3.into_rust_if_some("ProtoMyStruct::field_3")?,
            field_4: proto.field_4.into_rust_if_some("ProtoMyStruct::field_4")?,
            field_5: proto.field_5.into_rust()?,
            field_6: proto.field_6.into_rust()?,
            field_7: proto.field_7.into_rust()?,
        })
    }
}

impl ProtoMapEntry<GlobalId, NaiveDate> for proto_my_struct::ProtoField7Entry {
    fn from_rust<'a>(entry: (&'a GlobalId, &'a NaiveDate)) -> Self {
        Self {
            key: Some(entry.0.into_proto()),
            value: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, NaiveDate), TryFromProtoError> {
        let key = self.key.into_rust_if_some("ProtoField7Entry::key")?;
        let value = self.value.into_rust_if_some("ProtoField7Entry::value")?;
        Ok((key, value))
    }
}

impl RustType<ProtoMyEnum> for MyEnum {
    fn into_proto(&self) -> ProtoMyEnum {
        use proto_my_enum::Kind::*;

        ProtoMyEnum {
            kind: Some(match self {
                MyEnum::Var1(x) => Var1(x.clone()),
                MyEnum::Var2(x) => Var2(x.into_proto()),
                MyEnum::Var3(x) => Var3(x.into_proto()),
                MyEnum::Var4(x) => Var4(x.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoMyEnum) -> Result<Self, TryFromProtoError> {
        use proto_my_enum::Kind::*;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoMyEnum::kind"))?;

        Ok(match kind {
            Var1(x) => MyEnum::Var1(x),
            Var2(x) => MyEnum::Var2(x.into_rust()?),
            Var3(x) => MyEnum::Var3(x.into_rust()?),
            Var4(x) => MyEnum::Var4(x.into_rust()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;

    // snip

    proptest! {
        // use 64 instead of the default (256) cases for these tests
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn my_struct_protobuf_roundtrip(expect in any::<MyStruct>()) {
            let actual = protobuf_roundtrip::<_, ProtoMyStruct>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        #[test]
        fn my_enum_protobuf_roundtrip(expect in any::<MyEnum>()) {
            let actual = protobuf_roundtrip::<_, ProtoMyEnum>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
