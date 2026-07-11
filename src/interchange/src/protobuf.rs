// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use anyhow::{Context, anyhow, bail};
use mz_ore::str::StrExt;
use mz_repr::{ColumnName, Datum, Row, RowPacker, SqlColumnType, SqlScalarType};
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Value,
};

/// Maximum Protobuf message nesting depth accepted when deriving a relation
/// type. A message nested deeper than this is rejected rather than recursed
/// into, so that a pathological (non-cyclic) message chain cannot overflow the
/// stack while deriving the source's `RelationDesc`.
const MAX_MESSAGE_NESTING_DEPTH: usize = 128;

/// Maximum Protobuf wire nesting depth accepted when decoding a
/// `FileDescriptorSet`.
///
/// `DescriptorPool::decode` decodes the descriptor set recursively, descending
/// once per level of wire nesting. `DescriptorProto.nested_type` is
/// self-referential and unknown group fields are skipped recursively, so a
/// deeply nested (but cheap to encode) descriptor set drives that recursion
/// arbitrarily deep. The workspace builds Prost with `no-recursion-limit`, so
/// the recursion is unbounded and such input aborts the process with a stack
/// overflow before any of our own validation runs.
///
/// We therefore reject descriptor sets nested deeper than this on the wire,
/// before decoding them. This is distinct from [`MAX_MESSAGE_NESTING_DEPTH`],
/// which bounds how far message *type references* (`m0 -> m1 -> ...` by name)
/// are followed while deriving the relation type. Those references are a flat
/// list on the wire, so a reference chain does not nest here.
const MAX_DESCRIPTOR_WIRE_NESTING_DEPTH: usize = 128;

/// A decoded description of the schema of a Protobuf message.
#[derive(Debug, PartialEq)]
pub struct DecodedDescriptors {
    message_descriptor: MessageDescriptor,
    columns: Vec<(ColumnName, SqlColumnType)>,
    message_name: String,
}

impl DecodedDescriptors {
    /// Builds a `DecodedDescriptors` from an encoded `FileDescriptorSet` and
    /// the fully qualified name of a message inside that file descriptor set.
    pub fn from_bytes(bytes: &[u8], message_name: String) -> Result<Self, anyhow::Error> {
        // Reject pathologically nested input before `DescriptorPool::decode`
        // recurses into it and overflows the stack. See
        // `MAX_DESCRIPTOR_WIRE_NESTING_DEPTH`.
        check_descriptor_set_nesting_depth(bytes)?;
        let fds = DescriptorPool::decode(bytes).context("decoding file descriptor set")?;
        let message_descriptor = fds.get_message_by_name(&message_name).ok_or_else(|| {
            anyhow!(
                "protobuf message {} not found in file descriptor set",
                message_name.quoted(),
            )
        })?;
        let mut seen_messages = BTreeSet::new();
        seen_messages.insert(message_descriptor.name().to_owned());
        let mut columns = vec![];
        for field in message_descriptor.fields() {
            let name = ColumnName::from(field.name());
            let ty = derive_column_type(&mut seen_messages, &field)?;
            columns.push((name, ty))
        }
        Ok(DecodedDescriptors {
            message_descriptor,
            columns,
            message_name,
        })
    }

    /// Describes the columns in the message.
    ///
    /// In other words, the return value describes the shape of the rows that
    /// will be produced by a [`Decoder`] constructed from this
    /// `DecodedDescriptors`.
    pub fn columns(&self) -> &[(ColumnName, SqlColumnType)] {
        &self.columns
    }
}

/// Decodes a particular Protobuf message from its wire format.
#[derive(Debug)]
pub struct Decoder {
    descriptors: DecodedDescriptors,
    row: Row,
    confluent_wire_format: bool,
}

impl Decoder {
    /// Constructs a decoder for a particular Protobuf message.
    pub fn new(
        descriptors: DecodedDescriptors,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(Decoder {
            descriptors,
            row: Row::default(),
            confluent_wire_format,
        })
    }

    /// Decodes the encoded Protobuf message into a [`Row`].
    pub fn decode(&mut self, mut bytes: &[u8]) -> Result<Option<Row>, anyhow::Error> {
        if self.confluent_wire_format {
            // We support Protobuf schema evolution by ignoring the schema that
            // the message was written with and attempting to decode into the
            // schema we know about. As long as the new schema has been evolved
            // according to the Protobuf evolution rules [0], this produces
            // sensible and desirable results.
            //
            // There is the possibility that the message has been written with
            // an incompatible schema, but this is relatively unlikely as the
            // schema registry enforces compatible evolution by default. We
            // don't bother to perform our own compatibility checks because the
            // rules are complex and the Protobuf format is self-describing
            // enough that decoding an Protobuf message with an incompatible
            // schema is handled gracefully (e.g., no accidentally massive
            // allocations).
            //
            // [0]: https://developers.google.com/protocol-buffers/docs/overview
            let (_schema_id, adjusted_bytes) = crate::confluent::extract_protobuf_header(bytes)?;
            bytes = adjusted_bytes;
        }
        let message = DynamicMessage::decode(self.descriptors.message_descriptor.clone(), bytes)?;
        let mut packer = self.row.packer();
        pack_message(&mut packer, &message)?;
        Ok(Some(self.row.clone()))
    }
}

fn derive_column_type(
    seen_messages: &mut BTreeSet<String>,
    field: &FieldDescriptor,
) -> Result<SqlColumnType, anyhow::Error> {
    if field.is_map() {
        bail!("Protobuf map fields are not supported");
    }

    let ty = derive_inner_type(seen_messages, field.kind())?;
    if field.is_list() {
        Ok(SqlColumnType {
            nullable: false,
            scalar_type: SqlScalarType::List {
                element_type: Box::new(ty.scalar_type),
                custom_id: None,
            },
        })
    } else {
        Ok(ty)
    }
}

fn derive_inner_type(
    seen_messages: &mut BTreeSet<String>,
    ty: Kind,
) -> Result<SqlColumnType, anyhow::Error> {
    match ty {
        Kind::Bool => Ok(SqlScalarType::Bool.nullable(false)),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Ok(SqlScalarType::Int32.nullable(false)),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => Ok(SqlScalarType::Int64.nullable(false)),
        Kind::Uint32 | Kind::Fixed32 => Ok(SqlScalarType::UInt32.nullable(false)),
        Kind::Uint64 | Kind::Fixed64 => Ok(SqlScalarType::UInt64.nullable(false)),
        Kind::Float => Ok(SqlScalarType::Float32.nullable(false)),
        Kind::Double => Ok(SqlScalarType::Float64.nullable(false)),
        Kind::String => Ok(SqlScalarType::String.nullable(false)),
        Kind::Bytes => Ok(SqlScalarType::Bytes.nullable(false)),
        Kind::Enum(_) => Ok(SqlScalarType::String.nullable(false)),
        Kind::Message(m) => {
            if seen_messages.contains(m.name()) {
                bail!("Recursive types are not supported: {}", m.name());
            }
            // `seen_messages` holds the ancestor messages currently being
            // resolved, so its length is the current nesting depth. Bounding it
            // prevents a stack overflow from a deeply nested (but non-cyclic)
            // message chain, whose descriptor set is a flat, cheap-to-encode
            // list of messages that reference each other by name.
            if seen_messages.len() >= MAX_MESSAGE_NESTING_DEPTH {
                bail!(
                    "Protobuf message nesting depth exceeds limit of {}",
                    MAX_MESSAGE_NESTING_DEPTH
                );
            }
            seen_messages.insert(m.name().to_owned());
            let mut fields = Vec::with_capacity(m.fields().len());
            for field in m.fields() {
                let column_name = ColumnName::from(field.name());
                let column_type = derive_column_type(seen_messages, &field)?;
                fields.push((column_name, column_type))
            }
            seen_messages.remove(m.name());
            let ty = SqlScalarType::Record {
                fields: fields.into(),
                custom_id: None,
            };
            Ok(ty.nullable(true))
        }
    }
}

/// The `descriptor.proto` message type that a region of the wire is being
/// parsed as. [`check_descriptor_set_nesting_depth`] uses it to descend only
/// into fields that are message-typed, mirroring what `DescriptorPool::decode`
/// recurses into.
#[derive(Clone, Copy)]
enum WireCtx {
    FileDescriptorSet,
    FileDescriptorProto,
    DescriptorProto,
    ExtensionRange,
    FieldDescriptorProto,
    OneofDescriptorProto,
    EnumDescriptorProto,
    EnumValueDescriptorProto,
    ServiceDescriptorProto,
    MethodDescriptorProto,
    /// Any of the `*Options` messages. The only message-typed field they carry
    /// that matters here is `uninterpreted_option` (tag 999).
    Options,
    UninterpretedOption,
    SourceCodeInfo,
    /// A message with no message-typed fields, or an opaque group region. Only
    /// nested groups increase depth from here.
    Leaf,
}

impl WireCtx {
    /// Returns the context in which to parse the payload of the message-typed
    /// field with the given `tag`, or `None` if the field is not message-typed
    /// in this context (a scalar, `string`, `bytes`, or unknown field), in which
    /// case the scan treats the payload as an opaque leaf.
    ///
    /// This encodes the message structure of the `descriptor.proto` schema that
    /// `prost-reflect` decodes. `DescriptorPool::decode` recurses into exactly
    /// these fields, so mirroring them keeps the scan's depth in lockstep with
    /// the decoder's recursion depth.
    fn child(self, tag: u32) -> Option<WireCtx> {
        use WireCtx::*;
        Some(match (self, tag) {
            (FileDescriptorSet, 1) => FileDescriptorProto,
            (FileDescriptorProto, 4) => DescriptorProto,
            (FileDescriptorProto, 5) => EnumDescriptorProto,
            (FileDescriptorProto, 6) => ServiceDescriptorProto,
            (FileDescriptorProto, 7) => FieldDescriptorProto,
            (FileDescriptorProto, 8) => Options,
            (FileDescriptorProto, 9) => SourceCodeInfo,
            (DescriptorProto, 2) => FieldDescriptorProto,
            (DescriptorProto, 3) => DescriptorProto,
            (DescriptorProto, 4) => EnumDescriptorProto,
            (DescriptorProto, 5) => ExtensionRange,
            (DescriptorProto, 6) => FieldDescriptorProto,
            (DescriptorProto, 7) => Options,
            (DescriptorProto, 8) => OneofDescriptorProto,
            (DescriptorProto, 9) => Leaf, // ReservedRange
            (ExtensionRange, 3) => Options,
            (FieldDescriptorProto, 8) => Options,
            (OneofDescriptorProto, 2) => Options,
            (EnumDescriptorProto, 2) => EnumValueDescriptorProto,
            (EnumDescriptorProto, 3) => Options,
            (EnumDescriptorProto, 4) => Leaf, // EnumReservedRange
            (EnumValueDescriptorProto, 3) => Options,
            (ServiceDescriptorProto, 2) => MethodDescriptorProto,
            (ServiceDescriptorProto, 3) => Options,
            (MethodDescriptorProto, 4) => Options,
            (Options, 999) => UninterpretedOption,
            (UninterpretedOption, 2) => Leaf, // NamePart
            (SourceCodeInfo, 1) => Leaf,      // Location
            _ => return None,
        })
    }
}

/// Rejects a `FileDescriptorSet` whose wire encoding nests messages or groups
/// deeper than [`MAX_DESCRIPTOR_WIRE_NESTING_DEPTH`], which would otherwise
/// overflow the stack inside `DescriptorPool::decode`.
///
/// `DescriptorPool::decode` recurses in exactly two ways, both unbounded because
/// the workspace builds Prost with `no-recursion-limit`:
///
///  * It recursively decodes message-typed fields. In `descriptor.proto` the
///    only self-referential message field is `DescriptorProto.nested_type`, so a
///    `nested_type` chain drives this recursion arbitrarily deep.
///  * It skips unknown group fields recursively (`prost::encoding::skip_field`
///    recurses once per level of nested group), so a chain of `StartGroup` keys
///    drives the recursion arbitrarily deep as well.
///
/// The scan mirrors that recursion with an explicit stack, so it cannot itself
/// overflow. It is schema-aware: it descends only into length-delimited fields
/// that are message-typed according to `descriptor.proto` (tracked by
/// [`WireCtx`]) and into groups. A length-delimited field that is a `string`,
/// `bytes`, or unknown field is skipped as an opaque leaf, because
/// `DescriptorPool::decode` does not parse its contents as a message either.
///
/// Treating opaque payloads as leaves is essential, not just an optimization: a
/// `string` may contain byte sequences that are valid protobuf keys (an option
/// string of `0x4b` bytes reads as a chain of `StartGroup` keys), so a scan that
/// parsed string contents as nested protobuf would reject valid, shallow
/// descriptors.
///
/// Because the scan descends into every message-typed field the decoder decodes
/// and into every group the decoder skips, any input that would drive the
/// decoder past the limit trips the limit here first.
fn check_descriptor_set_nesting_depth(bytes: &[u8]) -> Result<(), anyhow::Error> {
    use bytes::Buf;
    use prost::encoding::{WireType, decode_key, decode_varint};

    struct Frame {
        kind: FrameKind,
        /// The context in which fields inside this frame are parsed.
        ctx: WireCtx,
    }
    enum FrameKind {
        /// A length-delimited region that ends once `buf.remaining()` has
        /// dropped to this value.
        Len(usize),
        /// A group that ends at an `EndGroup` key carrying this tag.
        Group(u32),
    }

    // Skips the remainder of the innermost length-delimited region, treating it
    // as opaque bytes. Returns `false` when there is no enclosing
    // length-delimited region to recover into, in which case scanning must stop.
    let recover = |frames: &mut Vec<Frame>, buf: &mut &[u8]| -> bool {
        while let Some(frame) = frames.pop() {
            if let FrameKind::Len(end_remaining) = frame.kind {
                let skip = buf.remaining().saturating_sub(end_remaining);
                buf.advance(skip);
                return true;
            }
        }
        false
    };

    let mut buf: &[u8] = bytes;
    let mut frames: Vec<Frame> = Vec::new();

    loop {
        // Close every length-delimited region that ends at the current position.
        while let Some(Frame {
            kind: FrameKind::Len(end_remaining),
            ..
        }) = frames.last()
        {
            if buf.remaining() <= *end_remaining {
                frames.pop();
            } else {
                break;
            }
        }
        if !buf.has_remaining() {
            // Any frames still open mean truncated input, which
            // `DescriptorPool::decode` will reject. The depth is bounded either
            // way.
            return Ok(());
        }

        // The context for the field about to be read is that of the innermost
        // open frame, or the root `FileDescriptorSet` at the top level.
        let ctx = frames
            .last()
            .map_or(WireCtx::FileDescriptorSet, |frame| frame.ctx);

        let (tag, wire_type) = match decode_key(&mut buf) {
            Ok(key) => key,
            Err(_) => {
                if recover(&mut frames, &mut buf) {
                    continue;
                }
                return Ok(());
            }
        };

        match wire_type {
            WireType::Varint => {
                if decode_varint(&mut buf).is_err() {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
            }
            WireType::SixtyFourBit | WireType::ThirtyTwoBit => {
                let width = if wire_type == WireType::SixtyFourBit {
                    8
                } else {
                    4
                };
                if buf.remaining() < width {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
                buf.advance(width);
            }
            WireType::LengthDelimited => {
                let len = match decode_varint(&mut buf) {
                    Ok(len) => len,
                    Err(_) => {
                        if recover(&mut frames, &mut buf) {
                            continue;
                        }
                        return Ok(());
                    }
                };
                if len > buf.remaining() as u64 {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
                match ctx.child(tag) {
                    // A message-typed field: descend, mirroring the decoder
                    // recursing into the nested message.
                    Some(child) => {
                        if frames.len() >= MAX_DESCRIPTOR_WIRE_NESTING_DEPTH {
                            bail!(
                                "Protobuf descriptor set nesting depth exceeds limit of {}",
                                MAX_DESCRIPTOR_WIRE_NESTING_DEPTH
                            );
                        }
                        let end_remaining = buf.remaining() - len as usize;
                        frames.push(Frame {
                            kind: FrameKind::Len(end_remaining),
                            ctx: child,
                        });
                    }
                    // A string, bytes, scalar, or unknown field. The decoder does
                    // not parse its contents as a message, so skip it as an
                    // opaque leaf rather than recursing into it.
                    None => buf.advance(len as usize),
                }
            }
            WireType::StartGroup => {
                if frames.len() >= MAX_DESCRIPTOR_WIRE_NESTING_DEPTH {
                    bail!(
                        "Protobuf descriptor set nesting depth exceeds limit of {}",
                        MAX_DESCRIPTOR_WIRE_NESTING_DEPTH
                    );
                }
                // Group contents are skipped as unknown wire data by the decoder,
                // so only further nested groups increase depth from here.
                frames.push(Frame {
                    kind: FrameKind::Group(tag),
                    ctx: WireCtx::Leaf,
                });
            }
            WireType::EndGroup => match frames.last() {
                Some(Frame {
                    kind: FrameKind::Group(open_tag),
                    ..
                }) if *open_tag == tag => {
                    frames.pop();
                }
                _ => {
                    if recover(&mut frames, &mut buf) {
                        continue;
                    }
                    return Ok(());
                }
            },
        }
    }
}

fn pack_message(packer: &mut RowPacker, message: &DynamicMessage) -> Result<(), anyhow::Error> {
    for field_desc in message.descriptor().fields() {
        if !message.has_field(&field_desc) {
            if field_desc.cardinality() == Cardinality::Required {
                bail!(
                    "protobuf message missing required field {}",
                    field_desc.name()
                );
            }
            if field_desc.kind().as_message().is_some() && !field_desc.is_list() {
                packer.push(Datum::Null);
                continue;
            }
        }
        let value = message.get_field(&field_desc);
        pack_value(packer, &field_desc, &*value)?;
    }
    Ok(())
}

fn pack_value(
    packer: &mut RowPacker,
    field_desc: &FieldDescriptor,
    value: &Value,
) -> Result<(), anyhow::Error> {
    match value {
        Value::Bool(false) => packer.push(Datum::False),
        Value::Bool(true) => packer.push(Datum::True),
        Value::I32(i) => packer.push(Datum::Int32(*i)),
        Value::I64(i) => packer.push(Datum::Int64(*i)),
        Value::U32(i) => packer.push(Datum::UInt32(*i)),
        Value::U64(i) => packer.push(Datum::UInt64(*i)),
        Value::F32(f) => packer.push(Datum::Float32((*f).into())),
        Value::F64(f) => packer.push(Datum::Float64((*f).into())),
        Value::String(s) => packer.push(Datum::String(s)),
        Value::Bytes(s) => packer.push(Datum::Bytes(s)),
        Value::EnumNumber(i) => {
            let kind = field_desc.kind();
            let enum_desc = kind.as_enum().ok_or_else(|| {
                anyhow!(
                    "internal error: decoding protobuf: field {} missing enum descriptor",
                    field_desc.name()
                )
            })?;
            let value = enum_desc.get_value(*i).ok_or_else(|| {
                anyhow!(
                    "error decoding protobuf: unknown enum value {} while decoding field {}",
                    i,
                    field_desc.name()
                )
            })?;
            packer.push(Datum::String(value.name()));
        }
        Value::Message(m) => packer.push_list_with(|packer| pack_message(packer, m))?,
        Value::List(values) => {
            packer.push_list_with(|packer| {
                for value in values {
                    pack_value(packer, field_desc, value)?;
                }
                Ok::<_, anyhow::Error>(())
            })?;
        }
        Value::Map(_) => bail!(
            "internal error: unexpected value while decoding protobuf message: {:?}",
            value
        ),
    }
    Ok(())
}

#[cfg(test)]
mod stack_overflow_verification {
    // Regression test: a deep, non-cyclic protobuf message chain
    // (`m0 -> m1 -> ... -> mN`) must not overflow the stack while deriving the
    // relation type in `DecodedDescriptors::from_bytes` (reached during
    // `CREATE SOURCE ... FORMAT PROTOBUF`). `derive_inner_type` bounds the
    // nesting depth, so this returns a graceful error rather than aborting the
    // process with a stack overflow.
    use prost::Message;
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, FileOptions,
    };

    use super::{DecodedDescriptors, MAX_DESCRIPTOR_WIRE_NESTING_DEPTH};

    fn deep_chain_fds(n: usize) -> Vec<u8> {
        let message_type = (0..n)
            .map(|i| {
                let field = if i + 1 < n {
                    FieldDescriptorProto {
                        name: Some("f".into()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(format!(".test.m{}", i + 1)),
                        ..Default::default()
                    }
                } else {
                    FieldDescriptorProto {
                        name: Some("f".into()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    }
                };
                DescriptorProto {
                    name: Some(format!("m{}", i)),
                    field: vec![field],
                    ..Default::default()
                }
            })
            .collect();
        let file = FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            message_type,
            syntax: Some("proto2".into()),
            ..Default::default()
        };
        FileDescriptorSet { file: vec![file] }.encode_to_vec()
    }

    #[mz_ore::test]
    fn deep_chain_does_not_overflow() {
        let bytes = deep_chain_fds(50_000);
        // Must return an error (nesting-depth limit), not abort with a stack
        // overflow. Before the fix this recursed once per message and aborted.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_err(),
            "expected a nesting-depth error for a deep message chain"
        );
    }

    /// The `type_name` reference chain above is flat on the wire, so it stresses
    /// the relation-type derivation. A `DescriptorProto.nested_type` chain is
    /// instead recursive on the wire, so it stresses `DescriptorPool::decode`
    /// itself, which decodes the descriptor set recursively without a depth
    /// limit (`no-recursion-limit`). This must be rejected before that decode
    /// overflows the stack.
    fn deep_nested_type_fds(depth: usize) -> Vec<u8> {
        use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint};

        // Build the wire encoding directly. Materializing the nested
        // `DescriptorProto` values would overflow Prost's *encoder* (and the
        // recursive `Drop`) for the same reason we are guarding the decoder.
        //
        // `lens[k]` is the encoded size of the `DescriptorProto` nested `k`
        // levels deep. Level 0 is an empty message (0 bytes); each further level
        // wraps the previous one in a single `nested_type` field (field 3).
        const NESTED_TYPE_FIELD: u32 = 3;
        let key_len = {
            let mut probe = vec![];
            encode_key(NESTED_TYPE_FIELD, WireType::LengthDelimited, &mut probe);
            probe.len()
        };
        let mut lens = vec![0usize; depth + 1];
        for k in 1..=depth {
            lens[k] = key_len + encoded_len_varint(lens[k - 1] as u64) + lens[k - 1];
        }

        let mut dp = Vec::with_capacity(lens[depth]);
        for k in (1..=depth).rev() {
            encode_key(NESTED_TYPE_FIELD, WireType::LengthDelimited, &mut dp);
            encode_varint(lens[k - 1] as u64, &mut dp);
        }

        // Wrap the chain: FileDescriptorProto.message_type (field 4), then
        // FileDescriptorSet.file (field 1).
        let mut fdp = vec![];
        encode_key(4, WireType::LengthDelimited, &mut fdp);
        encode_varint(dp.len() as u64, &mut fdp);
        fdp.extend_from_slice(&dp);

        let mut fds = vec![];
        encode_key(1, WireType::LengthDelimited, &mut fds);
        encode_varint(fdp.len() as u64, &mut fds);
        fds.extend_from_slice(&fdp);
        fds
    }

    #[mz_ore::test]
    fn deep_nested_type_does_not_overflow() {
        let bytes = deep_nested_type_fds(50_000);
        // Must return an error (wire nesting-depth limit), not abort with a
        // stack overflow. Before the fix `DescriptorPool::decode` recursed once
        // per `nested_type` level and aborted the process.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_err(),
            "expected a nesting-depth error for a deeply nested descriptor"
        );
    }

    /// A valid, shallow descriptor whose `FileOptions.java_package` is a long
    /// string of `0x4b` ('K') bytes. Each `0x4b` is a valid protobuf key
    /// (`StartGroup`, tag 9), so a scanner that parsed opaque string contents as
    /// nested protobuf would count one level of phantom nesting per byte and
    /// reject this descriptor once the string exceeds the depth limit.
    fn message_like_option_bytes_fds() -> Vec<u8> {
        let options = FileOptions {
            java_package: Some("K".repeat(MAX_DESCRIPTOR_WIRE_NESTING_DEPTH * 2)),
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            message_type: vec![DescriptorProto {
                name: Some("m0".into()),
                ..Default::default()
            }],
            options: Some(options),
            syntax: Some("proto2".into()),
            ..Default::default()
        };
        FileDescriptorSet { file: vec![file] }.encode_to_vec()
    }

    #[mz_ore::test]
    fn message_like_option_bytes_accepted() {
        let bytes = message_like_option_bytes_fds();
        // The wire scan must treat `java_package` as an opaque string, not
        // recurse into it, so this valid shallow descriptor is accepted just as
        // `DescriptorPool::decode` accepts it.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_ok(),
            "valid descriptor with message-like option bytes must be accepted, got: {:?}",
            res.err()
        );
    }

    /// A chain of `depth` bare `StartGroup` keys at the top level of the
    /// descriptor set. `DescriptorPool::decode` skips unknown groups recursively
    /// (`skip_field`), so this drives that recursion `depth` levels deep.
    fn deep_group_fds(depth: usize) -> Vec<u8> {
        use prost::encoding::{WireType, encode_key};

        let mut fds = vec![];
        // Tag 2 is not a field of `FileDescriptorSet` (which defines only field
        // 1), so the decoder skips it via `skip_field`, which recurses per nested
        // group. The scan bounds depth by the number of open groups regardless of
        // their tags.
        for _ in 0..depth {
            encode_key(2, WireType::StartGroup, &mut fds);
        }
        fds
    }

    #[mz_ore::test]
    fn deep_group_does_not_overflow() {
        let bytes = deep_group_fds(50_000);
        // Must return an error (wire nesting-depth limit), not abort with a stack
        // overflow inside `skip_field`'s recursive group skipping.
        let res = DecodedDescriptors::from_bytes(&bytes, ".test.m0".to_string());
        assert!(
            res.is_err(),
            "expected a nesting-depth error for a deeply nested group chain"
        );
    }
}
