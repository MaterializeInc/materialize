# Developer guide: Command and Response Binary Encoding

This guide is intended for developers that want to extend or modify the set of command and response types that comprise the APIs used between `materialized`, `dataflowd`, and `storaged`.
As part of this process, one also needs to:

1. add Protobuf-based serialization support for new types, and
1. ensure that the deserialization is backwards-compatible.

This guide currently focuses primarily on (1).
Details for (2) will be added as we accumulate more knowledge.

## Overview

This process of adding Protobuf-based serialization support for a new Rust type `$T` consists of the following <a name="implementation-steps"></a>**implementation steps**:

1. Define a new Rust type `$T`.
1. Define a Protobuf message type `Proto$T` (a.k.a. *the Protobuf representation of `$T`*) and compile it to Rust with [`prost`](https://github.com/tokio-rs/prost).
1. Implement a pair mappings that convert between `$T` and `Proto$T`.

If `$T` needs to be added to `mz_expr::foo::bar`, the the source code of the `mz_expr` crate needs to be adapted as follows.

- `expr` - crate root folder.
  - `build.rs` - contains `prost_build` instructions for compiling all `*.proto` files in the crate into `*.rs` source code.
    - [Add instructions for compiling `foo/bar.proto`.](#extending-buildrs)
  - `src` - crate sources folder.
    - `foo/bar.proto` - contains Protobuf definitions `Proto$T` for types `$T` located in `foo/bar/mod.rs`.
      - [Add a Protobuf message definition for `Proto$T`.](#defining-a-protobuf-message-for-protot)
    - `foo/bar/mod.rs` - contains Rust definitions `$T` and `Proto$T` and associated traits.
      - [Add the Rust definition of `$T`.](#defining-a-rust-typet)
      - [Include the Rust code of the generated `Proto$T` type.](#including-rust-sources-generated-by-prost)
      - [Implement `$T ⇔ Proto$T` mappings.](#implementing-t--protot-mappings)
      - [Add unit tests for `$T`.](#adding-unit-tests-for-t)

The following sections contain details for of the above each action items.

## Defining a Rust type`$T`

We consider two main cases for `$T` - structs and enums.
Here are the two definitions from `expr/src/foo/bar/mod.rs` to be used as a running example.

```rust
use chrono::NaiveDate;
use mz_repr::adt::char::CharLength;

// `$T` is a struct
#[derive(Debug)]
pub struct MyStruct {
    pub field_1: u64,
    pub field_2: usize,
    pub field_3: CharLength,
    pub field_4: NaiveDate,
}

// `$T` is an enum
#[derive(Debug)]
pub enum MyEnum {
    Var1(u64),
    Var2(usize),
    Var3(CharLength),
    Var4(NaiveDate),
}
```

The above examples also illustrate of the <a name="type-classes"></a>**classes of nested Rust types** that one may encounter:

<ol type="a">
  <li>Primitive types that have a Protobuf counterpart (such as <code>u64</code>).</li>
  <li>Primitive types that don't have a Protobuf counterpart (such as <code>usize</code>).</li>
  <li>Complex types that are defined by us (such as <code>MyLibType</code>).</li>
  <li>Complex types that are not defined by us (such as <code>DateTime</code>).</li>
</ol>

The problem of encoding `$T` in a Protobuf-based binary format thereby decomposes into the problem of encoding instance of each of the above four classes.
The following rules apply in general:

- Types from [class (a)](#type-classes) are trivially represented by their existing Protobuf counterpart.
- Types from [class (c)](#type-classes) are the primary focus of this guide.
- Types from [class (b)](#type-classes) only care about [implementation step (3)](#implementation-steps), and types from [class (d)](#implementation-steps) don't care about [step (1)](#implementation-steps).
- Types from [classes (b) or (d)](#type-classes) require a slightly different implementation mechanism for [step (3)](#implementation-steps) compared to types from [class (c)](#type-classes).
  The [`chrono` types PR](https://github.com/MaterializeInc/materialize/pull/11801) is a good example of the latter.

## Defining a `Protobuf` message for `Proto$T`

This step is only needed if `$T` is a complex type ([classes (c) or (d)](#type-classes)).
The initial message definition of `Proto$T` can be derived schematically from the shape of `$T` (see [Appendix A](#appendix-a-deriving-an-initial-protobuf-message-for-t) for details).
Here are the example contents of `expr/src/foo/bar.proto` for the running examples from [the previous section](#defining-a-rust-typet).

```protobuf
syntax = "proto3";

import "repr/src/adt/char.proto";
import "repr/src/chrono.proto";

package mz_expr.foo.bar;

// `$T` is a struct
message ProtoMyStruct {
    uint64 field_1 = 1;
    uint64 field_2 = 2;
    mz_repr.adt.char.ProtoCharLength field_3 = 3;
    mz_repr.chrono.ProtoNaiveDate field_4 = 4;
}

// `$T` is an enum
message ProtoMyEnum {
    oneof kind {
        uint64 var1 = 1;
        uint64 var2 = 2;
        mz_repr.adt.char.ProtoCharLength var3 = 3;
        mz_repr.chrono.ProtoNaiveDate var4 = 4;
    }
}
```

## Extending `build.rs`

This step is only needed if `$T` is a complex type ([classes (c) or (d)](#type-classes)).

```rust
fn main() {
    prost_build::Config::new()
        // list paths to external types used in the compiled files
        .extern_path(".mz_repr.adt.char", "::mz_repr::adt::char")
        .extern_path(".mz_repr.chrono", "::mz_repr::chrono")
        // snip (...)
        // make the docstring linter happy
        .type_attribute(".", "#[allow(missing_docs)]")
        // list paths to `*.proto` files to be compiled
        .compile_protos(
            &[
                "expr/src/foo/bar.proto",
                // snip (...)
            ],
            &[".."],
        )
        .unwrap();
}
```

## Including Rust sources generated by `prost`

Add the following line right after the `use` section at the top of `expr/src/foo/bar/mod.rs`:

```rust
include!(concat!(env!("OUT_DIR"), "/mz_expr.foo.bar.rs"));
```

## Implementing `$T ⇔ Proto$T` mappings

For types from [class (b)](#type-classes), we need to implement [the `ProtoRepr` trait](https://dev.materialize.com/api/rust/mz_repr/proto/trait.ProtoRepr.html).
Here is the implementation for `usize` for example.

```rust
impl ProtoRepr for usize {
    type Repr = u64;

    fn into_proto(self: Self) -> Self::Repr {
        u64::cast_from(self)
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        usize::try_from(repr).map_err(|err| err.into())
    }
}
```

For types from [class (c)](#type-classes), we need to implement the `From<&$T> for Proto$T` and `TryFrom<Proto$T> for $T` traits.
For example, here are the implementations for `MyStruct`

```rust
impl From<&MyStruct> for ProtoMyStruct {
    fn from(x: &MyStruct) -> Self {
        ProtoMyStruct {
            field_1: x.field_1,
            field_2: x.field_2.into_proto(),
            field_3: Some((&x.field_3).into()),
            field_4: Some((&x.field_4).into_proto()),
        }
    }
}

impl TryFrom<ProtoMyStruct> for MyStruct {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoMyStruct) -> Result<Self, Self::Error> {
        Ok(MyStruct {
            field_1: x.field_1,
            field_2: ProtoRepr::from_proto(x.field_2)?,
            field_3: x.field_3.try_into_if_some("ProtoMyStruct::field_3")?,
            field_4: x.field_4.from_proto_if_some("ProtoMyStruct::field_3")?,
        })
    }
}
```

and `MyEnum`.

```rust
impl From<&MyEnum> for ProtoMyEnum {
    fn from(x: &MyEnum) -> Self {
        use proto_my_enum::Kind::*;
        use proto_my_enum::*;

        ProtoMyEnum {
            kind: Some(match x {
                MyEnum::Var1(x) => Var1(x.clone()),
                MyEnum::Var2(x) => Var2(x.into_proto()),
                MyEnum::Var3(x) => Var3(x.into()),
                MyEnum::Var4(x) => Var4(x.into_proto()),
            }),
        }
    }
}

impl TryFrom<ProtoMyEnum> for MyEnum {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoMyEnum) -> Result<Self, Self::Error> {
        use proto_my_enum::Kind::*;
        use proto_my_enum::*;

        let kind = x
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoMyEnum::kind"))?;

        Ok(match kind {
            Var1(x) => MyEnum::Var1(x),
            Var2(x) => MyEnum::Var2(ProtoRepr::from_proto(x)?),
            Var3(x) => MyEnum::Var3(x.try_into()?),
            Var4(x) => MyEnum::Var4(ProtoRepr::from_proto(x)?),
        })
    }
}
```

For types from [class (d)](#type-classes), we need to implement [the `ProtoRepr` trait](https://dev.materialize.com/api/rust/mz_repr/proto/trait.ProtoRepr.html) as well.
Here is the implementation for `NaiveDate` for example.

```rust
impl ProtoRepr for NaiveDate {
    type Repr = ProtoNaiveDate;

    fn into_proto(self: Self) -> Self::Repr {
        ProtoNaiveDate {
            year: self.year(),
            ordinal: self.ordinal(),
        }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        NaiveDate::from_yo_opt(repr.year, repr.ordinal).ok_or_else(|| {
            TryFromProtoError::DateConversionError(format!(
                "NaiveDate::from_yo_opt({},{}) failed",
                repr.year, repr.ordinal
            ))
        })
    }
}
```

See also [Appendix B](#appendix-b-common-patterns-in-conversion-traits) for commonly used implementation patterns for various container types.

## Adding unit tests for `$T`

Unit tests for Protobuf encoding support rely on [the `proptest` library](https://altsysrq.github.io/proptest-book/intro.html).
In order add a test for a new type, follow these steps.

### Implementing [`proptest::Arbitrary`](https://docs.rs/proptest/latest/proptest/arbitrary/trait.Arbitrary.html) for `$T`

Implement [`proptest::Arbitrary`](https://docs.rs/proptest/latest/proptest/arbitrary/trait.Arbitrary.html) for your Rust type `$T`.

- For [class (a) and (b)](#type-classes) types the trait is already implemented by `proptest`.
- For [class (c)](#type-classes) types with relatively simple structure, one can use the `proptest_derive::Arbitrary` derive macro ([example](https://github.com/MaterializeInc/materialize/pull/11812/files#diff-e4bb64025b518e3405a4cb1cbe27910600c750cb32335731c56b59ee6295958fR21)).
- For [class (c)](#type-classes) types with vectors, recursive, or deeply-nested structure a custom `Arbitrary` implementation is required ([example](https://github.com/MaterializeInc/materialize/pull/12353/files#diff-8cb017eb837d9b1fb8253f9b0d0ddc478858a91a8579a163a8bb11a00577f85eR174-R189)).
- For [class (d)](#type-classes) types a strategy constructor should be used instead ([example](https://github.com/MaterializeInc/materialize/pull/11801/files#diff-4ecbf4683c5f2c03aa3e4d654c14bb4a246b0f34b16e0da920369f649f7d1dfaR23-R39)).

Here are the derive-based `Arbitrary` implementations for `MyStruct` and `MyEnum`.

```rust
use chrono::NaiveDate;
use mz_repr::adt::char::CharLength;
use mz_repr::chrono::any_naive_date;
use mz_repr::proto::*;
use proptest_derive::Arbitrary;

// `$T` is a struct
#[derive(Arbitrary, Debug, PartialEq, Eq, Hash)]
pub struct MyStruct {
    pub field_1: u64,
    pub field_2: usize,
    pub field_3: CharLength,
    #[proptest(strategy = "any_naive_date()")]
    pub field_4: NaiveDate,
}

// `$T` is an enum
#[derive(Arbitrary, Debug, PartialEq, Eq, Hash)]
pub enum MyEnum {
    Var1(u64),
    Var2(usize),
    Var3(CharLength),
    Var4(#[proptest(strategy = "any_naive_date()")] NaiveDate),
}
```

### Creating a `protobuf_roundtrip` test

Instantiate the following test function template in the `tests` submodule of the module containing `$T`.

```rust
#[test]
fn $t_protobuf_roundtrip(expect in any::<$T>()) {
    let actual = protobuf_roundtrip::<_, Proto$T>(&expect);
    assert!(actual.is_ok());
    assert_eq!(actual.unwrap(), expect);
}
```

Note that you might need to reduce the number of test cases [with a custom `ProptestConfig`](https://altsysrq.github.io/proptest-book/proptest/tutorial/config.html) in order to keep the test runtime under control.
Here are the tests for `MyStruct` and `MyEnum`.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

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
```

## Appendix A: Deriving an initial Protobuf message for `$T`

The following table summarizes rules for deriving the message definition for `Proto$T` based on the structure of `$T`.
We use double square brackets `〚$T〛` to denote the Protobuf type derived from `$T`.

<table>
  <tr>
   <td colspan="3"><strong>Rules for translating a Rust structure to a Protobuf structure <code>〚—〛</code></strong></td>
  </tr>
  <tr>
   <td><strong><code>$T</code></strong></td>
   <td><strong><code>〚$T〛</code></strong></td>
   <td><strong>Comments</strong></td>
  </tr>
  <tr>
   <td valign=top>
<pre>enum $T {
  Var1(...),
  Var2(...),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    〚$Var1〛 var_1 = 1;
    〚$Var2〛 var_2 = 2;
  }
}</pre>
   </td>
   <td valign=top>The variant types <code>〚$VarX〛</code>are determined by the structure of the variant.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $V3;
struct $V4();
enum $T {
  Var1,
  Var2(),
  Var3($V3),
  Var4($V4),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    google.protobuf.Empty var_1 = 1;
    google.protobuf.Empty var_2 = 2;
    google.protobuf.Empty var_3 = 3;
    google.protobuf.Empty var_4 = 4;
  }
}</pre>
   </td>
   <td valign=top>Nullary variants or unary variants of a nullary type have the <code>Empty</code> Protobuf type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>enum $T {
  Var1(usize),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    uint64 var_1 = 1;
  }
}</pre>
   </td>
   <td valign=top>Use the corresponding protobuf primitive type for Rust primitive types that have a Protobuf counterpart.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>enum $T {
  Var1(u64),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    uint64 var_1 = 1;
  }
}</pre>
   </td>
   <td valign=top>Use the Protobuf representation type for Rust primitive types that implement <code>ProtoRepr</code>.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>enum $T {
  Var1($V1),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    Proto$V1 var_1 = 1;  }
}</pre>
   </td>
   <td valign=top>Use <code>Proto$T1</code> if <code>$T1</code> is a complex type for which <code>Proto$T1</code> already exists.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $V1($U1);
enum $T {
  Var1($V1),
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  oneof kind {
    〚$U1〛 var_1 = 1;
  }
}</pre>
   </td>
   <td valign=top>Use the type that corresponds to <code>$U1</code> for a unary variant of a unary struct. If <code>$U1</code> is <code>Optional<_></code>, use the complex variant case (see the next item in the table).</td>
  </tr>
  <tr>
   <td valign=top>
<pre>enum $T {
  Var1 { .. },
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  message Proto$Var1 { 〚..〛 }
  oneof kind {
    Proto$Var1 var_1 = 1;
  }
}</pre>
   </td>
   <td valign=top>For complex variants, create a nested message type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $T {
  f1 : Option<$F1>,
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  Proto$F1 f1 = 1;
}</pre>
   </td>
   <td valign=top>If <code>$F1</code> is a complex type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $T {
  f1 : Option<$F1>,
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T {
  optional Proto$F1 f1 = 1;
}</pre>
   </td>
   <td valign=top>If <code>〚$F1〛</code> is a primitive Protobuf type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>HashMap<$K, $V></pre>
   </td>
   <td valign=top>
<pre>map<〚$K〛, 〚$V〛></pre>
   </td>
   <td valign=top>If <code>〚$K〛</code> is a primitive Protobuf type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>HashMap<$K, $V></pre>
   </td>
   <td valign=top>
<pre>repeated 〚($K, $V)〛</pre>
   </td>
   <td valign=top>If <code>〚$K〛</code> is not a primitive Protobuf type.</td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $T {
  f1 : vec<$T1>
}</pre>
   </td>
   <td valign=top>
<pre>Message Proto$T {
  repeated Proto$T1 f1 = 1;
}</pre>
   </td>
   <td valign=top></td>
  </tr>
  <tr>
   <td valign=top>
<pre>struct $T {
  f1 : vec&lt;vec&lt;$T1&gt;&gt;
}</pre>
   </td>
   <td valign=top>
<pre>message Proto$T1 { … }
message ProtoVec$T1 {
  repeated Proto$T1 value = 1;
}
message Proto$T {
  repeated ProtoF1Vec f1 = 1;
}</pre>
   </td>
   <td valign=top></td>
  </tr>
  <tr>
   <td valign=top>
<pre></pre>
   </td>
   <td valign=top>
<pre></pre>
   </td>
   <td valign=top></td>
  </tr>
</table>

## Appendix B: Common patterns in conversion traits

To convert `x: Vec<&T>`:
```rust
x.vector.iter().map(Into::into).collect()
```

To convert from a `x: Vec<Proto$T>`:
```rust
x.vector.into_iter().map(TryFrom::try_from).collect::<Result<_,_>>()?
```

To convert an `x: Option<&T>`:
```rust
x.opt.as_ref().map(Into::into)
```

To convert from an `x: Option<&T>`:
```rust
x.opt.map(|x| x.try_into()).transpose()?
```
