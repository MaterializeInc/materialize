// Copyright 2018 Flavien Raynaud.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is derived from the avro-rs project, available at
// https://github.com/flavray/avro-rs. It was incorporated
// directly into Materialize on March 3, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Logic handling reading from Avro format at user level.

use std::str::{from_utf8, FromStr};

use serde_json::from_slice;

use crate::decode::{decode, AvroRead};
use crate::error::{DecodeError, Error as AvroError};
use crate::schema::{
    resolve_schemas, FullName, NamedSchemaPiece, ParseSchemaError, RecordField,
    ResolvedDefaultValueField, SchemaNodeOrNamed, SchemaPiece, SchemaPieceOrNamed,
    SchemaPieceRefOrNamed,
};
use crate::schema::{ResolvedRecordField, Schema};
use crate::types::{AvroMap, Value};
use crate::util::{self};
use crate::{Codec, SchemaResolutionError};

use sha2::Sha256;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct Header {
    writer_schema: Schema,
    marker: [u8; 16],
    codec: Codec,
}

impl Header {
    pub fn from_reader<R: AvroRead>(reader: &mut R) -> Result<Header, AvroError> {
        let meta_schema = Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Map(Box::new(SchemaPiece::Bytes.into())).into(),
        };

        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;

        if buf != [b'O', b'b', b'j', 1u8] {
            return Err(AvroError::Decode(DecodeError::WrongHeaderMagic(buf)));
        }

        if let Value::Map(AvroMap(meta)) = decode(meta_schema.top_node(), reader)? {
            // TODO: surface original parse schema errors instead of coalescing them here
            let json = meta
                .get("avro.schema")
                .ok_or(AvroError::Decode(DecodeError::MissingAvroDotSchema))
                .and_then(|bytes| {
                    if let Value::Bytes(ref bytes) = *bytes {
                        from_slice(bytes.as_ref()).map_err(|e| {
                            AvroError::ParseSchema(ParseSchemaError::new(format!(
                                "unable to decode schema bytes: {}",
                                e
                            )))
                        })
                    } else {
                        unreachable!()
                    }
                })?;
            let writer_schema = Schema::parse(&json).map_err(|e| {
                ParseSchemaError::new(format!("unable to parse json as avro schema: {}", e))
            })?;

            let codec = meta
                .get("avro.codec")
                .map(|val| match val {
                    Value::Bytes(ref bytes) => from_utf8(bytes.as_ref())
                        .map_err(|_e| AvroError::Decode(DecodeError::CodecUtf8Error))
                        .and_then(|codec| {
                            Codec::from_str(codec).map_err(|_| {
                                AvroError::Decode(DecodeError::UnrecognizedCodec(codec.to_string()))
                            })
                        }),
                    _ => unreachable!(),
                })
                .unwrap_or(Ok(Codec::Null))?;

            let mut marker = [0u8; 16];
            reader.read_exact(&mut marker)?;

            Ok(Header {
                writer_schema,
                marker,
                codec,
            })
        } else {
            unreachable!()
        }
    }

    pub fn into_parts(self) -> (Schema, [u8; 16], Codec) {
        (self.writer_schema, self.marker, self.codec)
    }
}

pub struct Reader<R> {
    header: Header,
    inner: R,
    errored: bool,
    resolved_schema: Option<Schema>,
    messages_remaining: usize,
    // Internal buffering to reduce allocation.
    buf: Vec<u8>,
    buf_idx: usize,
}

/// An iterator over the `Block`s of a `Reader`
pub struct BlockIter<R> {
    inner: Reader<R>,
}

/// A block of Avro objects from an OCF file
#[derive(Debug, Clone)]
pub struct Block {
    /// The raw bytes for the block
    pub bytes: Vec<u8>,
    /// The number of Avro objects in the block
    pub len: usize,
}

impl<R: AvroRead> BlockIter<R> {
    pub fn with_schema(reader_schema: &Schema, inner: R) -> Result<Self, AvroError> {
        Ok(Self {
            inner: Reader::with_schema(reader_schema, inner)?,
        })
    }
}

impl<R: AvroRead> Iterator for BlockIter<R> {
    type Item = Result<Block, AvroError>;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(self.inner.is_empty());

        match self.inner.read_block_next() {
            Ok(()) => {
                if self.inner.is_empty() {
                    None
                } else {
                    let bytes = std::mem::take(&mut self.inner.buf);
                    let len = std::mem::take(&mut self.inner.messages_remaining);
                    Some(Ok(Block { bytes, len }))
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

impl<R: AvroRead> Reader<R> {
    /// Creates a `Reader` given something implementing the `tokio::io::AsyncRead` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn new(mut inner: R) -> Result<Reader<R>, AvroError> {
        let header = Header::from_reader(&mut inner)?;
        let reader = Reader {
            header,
            inner,
            errored: false,
            resolved_schema: None,
            messages_remaining: 0,
            buf: vec![],
            buf_idx: 0,
        };
        Ok(reader)
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `tokio::io::AsyncRead` trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub fn with_schema(reader_schema: &Schema, mut inner: R) -> Result<Reader<R>, AvroError> {
        let header = Header::from_reader(&mut inner)?;

        let writer_schema = &header.writer_schema;
        let resolved_schema = if reader_schema.fingerprint::<Sha256>().bytes
            != writer_schema.fingerprint::<Sha256>().bytes
        {
            Some(resolve_schemas(&writer_schema, reader_schema)?)
        } else {
            None
        };

        Ok(Reader {
            header,
            errored: false,
            resolved_schema,
            inner,
            messages_remaining: 0,
            buf: vec![],
            buf_idx: 0,
        })
    }

    /// Get a reference to the writer `Schema`.
    pub fn writer_schema(&self) -> &Schema {
        &self.header.writer_schema
    }

    /// Get a reference to the resolved schema
    /// (or just the writer schema, if no reader schema was provided
    ///  or the two schemas are identical)
    pub fn schema(&self) -> &Schema {
        match &self.resolved_schema {
            Some(schema) => schema,
            None => self.writer_schema(),
        }
    }

    #[inline]
    /// Read the next Avro value from the file, if one exists.
    pub fn read_next(&mut self) -> Result<Option<Value>, AvroError> {
        if self.is_empty() {
            self.read_block_next()?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();
        let schema = self.schema();
        let item = from_avro_datum(schema, &mut block_bytes)?;
        self.buf_idx += b_original - block_bytes.len();
        self.messages_remaining -= 1;
        Ok(Some(item))
    }

    fn is_empty(&self) -> bool {
        self.messages_remaining == 0
    }

    fn fill_buf(&mut self, n: usize) -> Result<(), AvroError> {
        // We don't have enough space in the buffer, need to grow it.
        if n >= self.buf.len() {
            self.buf.resize(n, 0);
        }

        self.inner.read_exact(&mut self.buf[..n])?;
        self.buf_idx = 0;
        Ok(())
    }

    fn read_block_next(&mut self) -> Result<(), AvroError> {
        assert!(self.is_empty(), "Expected self to be empty!");
        match util::read_long(&mut self.inner) {
            Ok(block_len) => {
                self.messages_remaining = block_len as usize;
                let block_bytes = util::read_long(&mut self.inner)?;
                self.fill_buf(block_bytes as usize)?;
                let mut marker = [0u8; 16];
                self.inner.read_exact(&mut marker)?;

                if marker != self.header.marker {
                    return Err(DecodeError::MismatchedBlockHeader {
                        expected: self.header.marker,
                        actual: marker,
                    }
                    .into());
                }

                // NOTE (JAB): This doesn't fit this Reader pattern very well.
                // `self.buf` is a growable buffer that is reused as the reader is iterated.
                // For non `Codec::Null` variants, `decompress` will allocate a new `Vec`
                // and replace `buf` with the new one, instead of reusing the same buffer.
                // We can address this by using some "limited read" type to decode directly
                // into the buffer. But this is fine, for now.
                self.header.codec.decompress(&mut self.buf)?;

                Ok(())
            }
            Err(e) => {
                if let AvroError::IO(std::io::ErrorKind::UnexpectedEof) = e {
                    // to not return any error in case we only finished to read cleanly from the stream
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }
}

impl<R: AvroRead> Iterator for Reader<R> {
    type Item = Result<Value, AvroError>;

    fn next(&mut self) -> Option<Self::Item> {
        // to prevent continuing to read after the first error occurs
        if self.errored {
            return None;
        };
        match self.read_next() {
            Ok(opt) => opt.map(Ok),
            Err(e) => {
                self.errored = true;
                Some(Err(e))
            }
        }
    }
}

pub struct SchemaResolver<'a> {
    pub named: Vec<Option<NamedSchemaPiece>>,
    pub indices: HashMap<FullName, usize>,
    pub human_readable_field_path: Vec<String>,
    pub current_human_readable_path_start: usize,
    pub writer_to_reader_names: HashMap<usize, usize>,
    pub reader_to_writer_names: HashMap<usize, usize>,
    pub reader_to_resolved_names: HashMap<usize, usize>,
    pub reader_fullnames: HashMap<usize, &'a FullName>,
    pub reader_schema: &'a Schema,
}

impl<'a> SchemaResolver<'a> {
    fn resolve_named(
        &mut self,
        writer: &Schema,
        reader: &Schema,
        writer_index: usize,
        reader_index: usize,
    ) -> Result<SchemaPiece, AvroError> {
        let ws = writer.lookup(writer_index);
        let rs = reader.lookup(reader_index);
        let typ = match (&ws.piece, &rs.piece) {
            (
                SchemaPiece::Record {
                    fields: w_fields,
                    lookup: w_lookup,
                    ..
                },
                SchemaPiece::Record {
                    fields: r_fields,
                    lookup: _r_lookup,
                    ..
                },
            ) => {
                let mut defaults = Vec::new();
                let mut fields: Vec<Option<RecordField>> = Vec::new();
                for (r_index, rf) in r_fields.iter().enumerate() {
                    match w_lookup.get(&rf.name) {
                        None => {
                            let default_field = match &rf.default {
                                Some(v) => ResolvedDefaultValueField {
                                    name: rf.name.clone(),
                                    doc: rf.doc.clone(),
                                    default: reader
                                        .top_node_or_named()
                                        .step(&rf.schema)
                                        .lookup()
                                        .json_to_value(v)?,
                                    order: rf.order.clone(),
                                    position: r_index,
                                },
                                None => {
                                    return Err(SchemaResolutionError::new(format!(
                                    "Reader field `{}.{}` not found in writer, and has no default",
                                    self.get_current_human_readable_path(),
                                    rf.name
                                ))
                                    .into())
                                }
                            };
                            defaults.push(default_field);
                        }
                        Some(w_index) => {
                            if fields.len() > *w_index && fields[*w_index].is_some() {
                                return Err(SchemaResolutionError::new(format!(
                                    "Duplicate field `{}.{}` in schema",
                                    self.get_current_human_readable_path(),
                                    rf.name
                                ))
                                .into());
                            }
                            let wf = &w_fields[*w_index];
                            let w_node = SchemaNodeOrNamed {
                                root: writer,
                                inner: wf.schema.as_ref(),
                            };
                            let r_node = SchemaNodeOrNamed {
                                root: reader,
                                inner: rf.schema.as_ref(),
                            };

                            self.human_readable_field_path.push(rf.name.clone());
                            let new_inner = self.resolve(w_node, r_node)?;
                            self.human_readable_field_path.pop();

                            let field = RecordField {
                                name: rf.name.clone(),
                                doc: rf.doc.clone(),
                                default: rf.default.clone(),
                                schema: new_inner,
                                order: rf.order.clone(),
                                position: r_index,
                            };
                            while fields.len() <= *w_index {
                                fields.push(None);
                            }
                            fields[*w_index] = Some(field)
                        }
                    }
                }
                while fields.len() < w_fields.len() {
                    fields.push(None);
                }
                let mut n_present = 0;
                let fields = fields
                    .into_iter()
                    .enumerate()
                    .map(|(i, rf)| match rf {
                        Some(rf) => {
                            n_present += 1;
                            ResolvedRecordField::Present(rf)
                        }
                        None => {
                            // Clone the chunk of the writer schema appearing here.
                            // We could probably be clever and avoid some cloning,
                            // but absolute highest performance probably isn't important for schema resolution.
                            //
                            // The cloned writer schema piece is needed to guide decoding of the value,
                            // since even though it doesn't appear in the reader schema it needs
                            // to be decoded to know where it ends.
                            //
                            // TODO -- We could try to come up with a "Dummy" schema variant
                            // that does only enough decoding to find the end of a value,
                            // and maybe save some time.
                            let writer_schema_piece = SchemaNodeOrNamed {
                                root: writer,
                                inner: w_fields[i].schema.as_ref(),
                            }
                            .to_schema();
                            ResolvedRecordField::Absent(writer_schema_piece)
                        }
                    })
                    .collect();
                let n_reader_fields = defaults.len() + n_present;
                SchemaPiece::ResolveRecord {
                    defaults,
                    fields,
                    n_reader_fields,
                }
            }
            (
                SchemaPiece::Enum {
                    symbols: w_symbols, ..
                },
                SchemaPiece::Enum {
                    symbols: r_symbols,
                    doc,
                    default_idx,
                },
            ) => {
                let r_map = r_symbols
                    .iter()
                    .enumerate()
                    .map(|(i, s)| (s, i))
                    .collect::<HashMap<_, _>>();
                let symbols = w_symbols
                    .iter()
                    .map(|s| {
                        r_map
                            .get(s)
                            .map(|i| (*i, s.clone()))
                            .ok_or_else(|| s.clone())
                    })
                    .collect();
                SchemaPiece::ResolveEnum {
                    doc: doc.clone(),
                    symbols,
                    default: default_idx.map(|i| (i, r_symbols[i].clone())),
                }
            }
            (SchemaPiece::Fixed { size: wsz }, SchemaPiece::Fixed { size: rsz }) => {
                if *wsz == *rsz {
                    SchemaPiece::Fixed { size: *wsz }
                } else {
                    return Err(SchemaResolutionError::new(format!(
                        "Fixed schema {:?}: sizes don't match ({}, {}) for field `{}`",
                        &rs.name,
                        wsz,
                        rsz,
                        self.get_current_human_readable_path(),
                    ))
                    .into());
                }
            }
            (
                SchemaPiece::Decimal {
                    precision: wp,
                    scale: wscale,
                    fixed_size: wsz,
                },
                SchemaPiece::Decimal {
                    precision: rp,
                    scale: rscale,
                    fixed_size: rsz,
                },
            ) => {
                if wp != rp {
                    return Err(SchemaResolutionError::new(format!(
                        "Decimal schema {:?}: precisions don't match: {}, {} for field `{}`",
                        &rs.name,
                        wp,
                        rp,
                        self.get_current_human_readable_path(),
                    ))
                    .into());
                }
                if wscale != rscale {
                    return Err(SchemaResolutionError::new(format!(
                        "Decimal schema {:?}: sizes don't match: {}, {} for field `{}`",
                        &rs.name,
                        wscale,
                        rscale,
                        self.get_current_human_readable_path(),
                    ))
                    .into());
                }
                if wsz != rsz {
                    return Err(SchemaResolutionError::new(format!(
                        "Decimal schema {:?}: sizes don't match: {:?}, {:?} for field `{}`",
                        &rs.name,
                        wsz,
                        rsz,
                        self.get_current_human_readable_path(),
                    ))
                    .into());
                }
                SchemaPiece::Decimal {
                    precision: *wp,
                    scale: *wscale,
                    fixed_size: *wsz,
                }
            }
            (SchemaPiece::Decimal { fixed_size, .. }, SchemaPiece::Fixed { size })
                if *fixed_size == Some(*size) =>
            {
                SchemaPiece::Fixed { size: *size }
            }
            (
                SchemaPiece::Fixed { size },
                SchemaPiece::Decimal {
                    precision,
                    scale,
                    fixed_size,
                },
            ) if *fixed_size == Some(*size) => SchemaPiece::Decimal {
                precision: *precision,
                scale: *scale,
                fixed_size: *fixed_size,
            },

            (_, SchemaPiece::ResolveRecord { .. })
            | (_, SchemaPiece::ResolveEnum { .. })
            | (SchemaPiece::ResolveRecord { .. }, _)
            | (SchemaPiece::ResolveEnum { .. }, _) => {
                return Err(SchemaResolutionError::new(
                    "Attempted to resolve an already resolved schema".to_string(),
                )
                .into());
            }

            (_wt, _rt) => {
                return Err(SchemaResolutionError::new(format!(
                    "Non-matching schemas: writer: {:?}, reader: {:?}",
                    ws.name, rs.name
                ))
                .into())
            }
        };
        Ok(typ)
    }

    pub fn resolve(
        &mut self,
        writer: SchemaNodeOrNamed,
        reader: SchemaNodeOrNamed,
    ) -> Result<SchemaPieceOrNamed, AvroError> {
        let previous_human_readable_path_start = self.current_human_readable_path_start;
        let (_, named_node) = reader.inner.get_piece_and_name(reader.root);
        if let Some(full_name) = named_node {
            self.current_human_readable_path_start = self.human_readable_field_path.len();
            self.human_readable_field_path.push(full_name.human_name());
        }

        let inner = match (writer.inner, reader.inner) {
            // Both schemas are unions - the most complicated case, but simpler than it looks.
            // For each variant in the writer, we attempt to find a matching variant in the reader,
            // either by type (for anonymous nodes) or by name (for named nodes).
            //
            // Having found a match, we resolve the writer variant against the reader variant,
            // and record it in the resolved node.
            //
            // If either no match is found, or resolution on the matches fails, it is not an error
            // -- it simply means that the corresponding entry in `permutation` will be `None`,
            // and reading will fail if that variant is expressed. But
            // reading variants that *do* match and resolve will still be possible.
            //
            // See the doc comment on `SchemaPiece::ResolveUnionUnion` for an explanation of the format of `permutation`.
            (
                SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(w_inner)),
                SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(r_inner)),
            ) => {
                let w2r = self.writer_to_reader_names.clone();
                // permuation[1] is Some((j, val)) iff the i'th writer variant
                // _matches_ the j'th reader variant
                // (i.e., it is the same primitive type, or the same kind of named type and has the same name, or a decimal with the same parameters)
                // and succuessfully _resolves_ against it,
                // and None otherwise.
                //
                // An example of types that match but don't resolve would be two records with the same name but incompatible fields.
                let permutation = w_inner
                    .variants()
                    .iter()
                    .map(|w_variant| {
                        let (r_idx, r_variant) =
                            r_inner.match_(w_variant, &w2r).ok_or_else(|| {
                                SchemaResolutionError::new(format!(
                                    "Failed to match writer union variant `{}` against any variant in the reader for field `{}`",
                                    w_variant.get_human_name(writer.root),
                                    self.get_current_human_readable_path()
                                ))
                            })?;
                        let resolved =
                            self.resolve(writer.step(w_variant), reader.step(r_variant))?;
                        Ok((r_idx, resolved))
                    })
                    .collect();
                let n_reader_variants = r_inner.variants().len();
                let reader_null_variant = r_inner
                    .variants()
                    .iter()
                    .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveUnionUnion {
                    permutation,
                    n_reader_variants,
                    reader_null_variant,
                })
            }
            // Writer is concrete; reader is union
            (other, SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(r_inner))) => {
                let n_reader_variants = r_inner.variants().len();
                let reader_null_variant = r_inner
                    .variants()
                    .iter()
                    .position(|v| v == &SchemaPieceOrNamed::Piece(SchemaPiece::Null));
                let (index, r_inner) = r_inner
                    .match_ref(other, &self.writer_to_reader_names)
                    .ok_or_else(|| {
                        SchemaResolutionError::new(
                            format!("No matching schema in reader union for writer type `{}` for field `{}`",
                                    other.get_human_name(writer.root),
                                    self.get_current_human_readable_path()))
                    })?;
                let inner = Box::new(self.resolve(writer.step_ref(other), reader.step(r_inner))?);
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveConcreteUnion {
                    index,
                    inner,
                    n_reader_variants,
                    reader_null_variant,
                })
            }
            // Writer is union; reader is concrete
            (SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(w_inner)), other) => {
                let (index, w_inner) = w_inner
                    .match_ref(other, &self.reader_to_writer_names)
                    .ok_or_else(|| {
                        SchemaResolutionError::new(
                            format!("No matching schema in writer union for reader type `{}` for field `{}`",
                                    other.get_human_name(writer.root),
                                    self.get_current_human_readable_path()))
                    })?;
                let inner = Box::new(self.resolve(writer.step(w_inner), reader.step_ref(other))?);
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveUnionConcrete { index, inner })
            }
            // Any other anonymous type.
            (SchemaPieceRefOrNamed::Piece(wp), SchemaPieceRefOrNamed::Piece(rp)) => {
                match (wp, rp) {
                    // Normally for types that are underlyingly "long", we just interpret them according to the reader schema.
                    // In this special case, it is better to interpret them according to the _writer_ schema:
                    // By treating the written value as millis, we will decode the same DateTime values as were written.
                    //
                    // For example: if a writer wrote milliseconds and a reader tries to read it as microseconds,
                    // it will be off by a factor of 1000 from the timestamp that the writer was intending to write
                    (SchemaPiece::TimestampMilli, SchemaPiece::TimestampMicro) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::TimestampMilli)
                    }
                    // See above
                    (SchemaPiece::TimestampMicro, SchemaPiece::TimestampMilli) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::TimestampMicro)
                    }
                    (SchemaPiece::Date, SchemaPiece::TimestampMilli)
                    | (SchemaPiece::Date, SchemaPiece::TimestampMicro) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveDateTimestamp)
                    }
                    (wp, rp) if wp.is_underlying_int() && rp.is_underlying_int() => {
                        SchemaPieceOrNamed::Piece(rp.clone()) // This clone is just a copy - none of the underlying int/long types own heap memory.
                    }
                    (wp, rp) if wp.is_underlying_long() && rp.is_underlying_long() => {
                        SchemaPieceOrNamed::Piece(rp.clone()) // see above comment
                    }
                    (wp, SchemaPiece::TimestampMilli) if wp.is_underlying_int() => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveIntTsMilli)
                    }
                    (wp, SchemaPiece::TimestampMicro) if wp.is_underlying_int() => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveIntTsMicro)
                    }
                    (SchemaPiece::Null, SchemaPiece::Null) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Null)
                    }
                    (SchemaPiece::Boolean, SchemaPiece::Boolean) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Boolean)
                    }
                    (SchemaPiece::Int, SchemaPiece::Long) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveIntLong)
                    }
                    (SchemaPiece::Int, SchemaPiece::Float) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveIntFloat)
                    }
                    (SchemaPiece::Int, SchemaPiece::Double) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveIntDouble)
                    }
                    (SchemaPiece::Long, SchemaPiece::Float) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveLongFloat)
                    }
                    (SchemaPiece::Long, SchemaPiece::Double) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveLongDouble)
                    }
                    (SchemaPiece::Float, SchemaPiece::Float) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Float)
                    }
                    (SchemaPiece::Float, SchemaPiece::Double) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::ResolveFloatDouble)
                    }
                    (SchemaPiece::Double, SchemaPiece::Double) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Double)
                    }
                    (b, SchemaPiece::Bytes)
                        if b == &SchemaPiece::Bytes || b == &SchemaPiece::String =>
                    {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Bytes)
                    }
                    (s, SchemaPiece::String)
                        if s == &SchemaPiece::String || s == &SchemaPiece::Bytes =>
                    {
                        SchemaPieceOrNamed::Piece(SchemaPiece::String)
                    }
                    (SchemaPiece::Array(w_inner), SchemaPiece::Array(r_inner)) => {
                        let inner =
                            self.resolve(writer.step(&**w_inner), reader.step(&**r_inner))?;
                        SchemaPieceOrNamed::Piece(SchemaPiece::Array(Box::new(inner)))
                    }
                    (SchemaPiece::Map(w_inner), SchemaPiece::Map(r_inner)) => {
                        let inner =
                            self.resolve(writer.step(&**w_inner), reader.step(&**r_inner))?;
                        SchemaPieceOrNamed::Piece(SchemaPiece::Map(Box::new(inner)))
                    }
                    (
                        SchemaPiece::Decimal {
                            precision: wp,
                            scale: ws,
                            fixed_size: wf,
                        },
                        SchemaPiece::Decimal {
                            precision: rp,
                            scale: rs,
                            fixed_size: rf,
                        },
                    ) => {
                        if wp == rp && ws == rs && wf == rf {
                            SchemaPieceOrNamed::Piece(SchemaPiece::Decimal {
                                precision: *wp,
                                scale: *ws,
                                fixed_size: *wf,
                            })
                        } else {
                            return Err(SchemaResolutionError::new(format!(
                                "Decimal types must match in precision, scale, and fixed size. \
                                Got ({:?}, {:?}, {:?}); ({:?}, {:?}. {:?}) for field `{}`",
                                wp,
                                ws,
                                wf,
                                rp,
                                rs,
                                rf,
                                self.get_current_human_readable_path(),
                            ))
                            .into());
                        }
                    }
                    (SchemaPiece::Decimal { fixed_size, .. }, SchemaPiece::Bytes)
                        if *fixed_size == None =>
                    {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Bytes)
                    }
                    // TODO [btv] We probably want to rethink what we're doing here, rather than just add
                    // a new branch for every possible "logical" type. Perhaps logical types with the
                    // same underlying type should always be resolvable to the reader schema's type?
                    (SchemaPiece::Json, SchemaPiece::Json) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Json)
                    }
                    (SchemaPiece::Uuid, SchemaPiece::Uuid) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Uuid)
                    }
                    (
                        SchemaPiece::Bytes,
                        SchemaPiece::Decimal {
                            precision,
                            scale,
                            fixed_size,
                        },
                    ) if *fixed_size == None => SchemaPieceOrNamed::Piece(SchemaPiece::Decimal {
                        precision: *precision,
                        scale: *scale,
                        fixed_size: *fixed_size,
                    }),
                    (ws, rs) => {
                        return Err(SchemaResolutionError::new(format!(
                            "Writer schema has type `{:?}`, but reader schema has type `{:?}` for field `{}`",
                            ws,
                            rs,
                            self.get_current_human_readable_path(),
                        ))
                        .into());
                    }
                }
            }
            // Named types
            (SchemaPieceRefOrNamed::Named(w_index), SchemaPieceRefOrNamed::Named(r_index)) => {
                if self.writer_to_reader_names.get(&w_index) != Some(&r_index) {
                    // The nodes in the two schemas have different names. Resolution fails.
                    let (w_name, r_name) = (
                        &writer.root.lookup(w_index).name,
                        &reader.root.lookup(r_index).name,
                    );
                    return Err(SchemaResolutionError::new(format!("Attempted to resolve writer schema node named {} against reader schema node named {}", w_name, r_name)).into());
                }
                // Check if we have already resolved the name previously, and if so, return a reference to
                // it (in the new schema's namespace).
                let idx = match self.reader_to_resolved_names.get(&r_index) {
                    Some(resolved) => *resolved,
                    None => {
                        // We have not resolved this name yet; do so, and record it in the set of named schemas.
                        // We need to push a placeholder beforehand, because schemas can be recursive;
                        // a schema nested under this one may reference it.
                        // A plausible example: {"type": "record", "name": "linked_list", "fields": [{"name": "next", "type": ["null", "linked_list"]}]}
                        // Thus, `self.reader_to_resolved_names` needs to be correct for this node's index *before* we traverse the nodes under it.
                        let resolved_idx = self.named.len();
                        self.reader_to_resolved_names.insert(r_index, resolved_idx);
                        self.named.push(None);
                        let piece =
                            match self.resolve_named(writer.root, reader.root, w_index, r_index) {
                                Ok(piece) => piece,
                                Err(e) => {
                                    // clean up the placeholder values that were added above.
                                    self.named.pop();
                                    self.reader_to_resolved_names.remove(&r_index);
                                    return Err(e);
                                }
                            };
                        let name = &self.reader_schema.named[r_index].name;
                        let ns = NamedSchemaPiece {
                            name: name.clone(),
                            piece,
                        };
                        self.named[resolved_idx] = Some(ns);
                        self.indices.insert(name.clone(), resolved_idx);

                        resolved_idx
                    }
                };
                SchemaPieceOrNamed::Named(idx)
            }
            (ws, rs) => {
                return Err(SchemaResolutionError::new(format!(
                    "Schemas don't match: {:?}, {:?} for field `{}`",
                    ws.get_piece_and_name(&writer.root).0,
                    rs.get_piece_and_name(&reader.root).0,
                    self.get_current_human_readable_path(),
                ))
                .into())
            }
        };
        if named_node.is_some() {
            self.human_readable_field_path.pop();
            self.current_human_readable_path_start = previous_human_readable_path_start;
        }
        Ok(inner)
    }

    fn get_current_human_readable_path(&self) -> String {
        return self.human_readable_field_path[self.current_human_readable_path_start..].join(".");
    }
}

/// Decode a `Value` encoded in Avro format given its `Schema` and anything implementing `io::Read`
/// to read from.
///
/// In case a reader `Schema` is provided, schema resolution will also be performed.
///
/// **NOTE** This function has a quite small niche of usage and does NOT take care of reading the
/// header and consecutive data blocks; use [`Reader`](struct.Reader.html) if you don't know what
/// you are doing, instead.
pub fn from_avro_datum<R: AvroRead>(schema: &Schema, reader: &mut R) -> Result<Value, AvroError> {
    let value = decode(schema.top_node(), reader)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Record, ToAvro};
    use crate::Reader;

    use std::io::Cursor;

    static SCHEMA: &str = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;
    static UNION_SCHEMA: &str = r#"
            ["null", "long"]
        "#;
    static ENCODED: &[u8] = &[
        79u8, 98u8, 106u8, 1u8, 4u8, 22u8, 97u8, 118u8, 114u8, 111u8, 46u8, 115u8, 99u8, 104u8,
        101u8, 109u8, 97u8, 222u8, 1u8, 123u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8,
        114u8, 101u8, 99u8, 111u8, 114u8, 100u8, 34u8, 44u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8,
        58u8, 34u8, 116u8, 101u8, 115u8, 116u8, 34u8, 44u8, 34u8, 102u8, 105u8, 101u8, 108u8,
        100u8, 115u8, 34u8, 58u8, 91u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8,
        97u8, 34u8, 44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 108u8, 111u8, 110u8,
        103u8, 34u8, 44u8, 34u8, 100u8, 101u8, 102u8, 97u8, 117u8, 108u8, 116u8, 34u8, 58u8, 52u8,
        50u8, 125u8, 44u8, 123u8, 34u8, 110u8, 97u8, 109u8, 101u8, 34u8, 58u8, 34u8, 98u8, 34u8,
        44u8, 34u8, 116u8, 121u8, 112u8, 101u8, 34u8, 58u8, 34u8, 115u8, 116u8, 114u8, 105u8,
        110u8, 103u8, 34u8, 125u8, 93u8, 125u8, 20u8, 97u8, 118u8, 114u8, 111u8, 46u8, 99u8, 111u8,
        100u8, 101u8, 99u8, 8u8, 110u8, 117u8, 108u8, 108u8, 0u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8, 4u8, 20u8, 54u8,
        6u8, 102u8, 111u8, 111u8, 84u8, 6u8, 98u8, 97u8, 114u8, 94u8, 61u8, 54u8, 221u8, 190u8,
        207u8, 108u8, 180u8, 158u8, 57u8, 114u8, 40u8, 173u8, 199u8, 228u8, 239u8,
    ];

    #[test]
    fn test_from_avro_datum() {
        let schema: Schema = SCHEMA.parse().unwrap();
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.avro();

        assert_eq!(from_avro_datum(&schema, &mut encoded).unwrap(), expected);
    }

    #[test]
    fn test_null_union() {
        let schema: Schema = UNION_SCHEMA.parse().unwrap();
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded).unwrap(),
            Value::Union {
                index: 1,
                inner: Box::new(Value::Long(0)),
                n_variants: 2,
                null_variant: Some(0)
            }
        );
    }

    #[test]
    fn test_reader_stream() {
        let schema: Schema = SCHEMA.parse().unwrap();
        let reader = Reader::with_schema(&schema, ENCODED).unwrap();

        let mut record1 = Record::new(schema.top_node()).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(schema.top_node()).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = vec![record1.avro(), record2.avro()];

        for (i, value) in reader.enumerate() {
            assert_eq!(value.unwrap(), expected[i]);
        }
    }

    #[test]
    fn test_reader_invalid_header() {
        let schema: Schema = SCHEMA.parse().unwrap();
        let invalid = ENCODED.iter().skip(1).copied().collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).is_err());
    }

    #[test]
    fn test_reader_invalid_block() {
        let schema: Schema = SCHEMA.parse().unwrap();
        let invalid = ENCODED
            .iter()
            .rev()
            .skip(19)
            .copied()
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let reader = Reader::with_schema(&schema, &invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_reader_empty_buffer() {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).is_err());
    }

    #[test]
    fn test_reader_only_header() {
        let invalid = ENCODED.iter().copied().take(165).collect::<Vec<u8>>();
        let reader = Reader::new(&invalid[..]).unwrap();
        for value in reader {
            assert!(value.is_err());
        }
    }

    #[test]
    fn test_resolution_nested_types_error() {
        let r = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": {"type": "record", "name": "com.materialize.bar", "fields": [{"name": "f1_1", "type": "int"}]}}
    ]
}
"#;
        let w = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": {"type": "record", "name": "com.materialize.bar", "fields": [{"name": "f1_1", "type": "double"}]}}
    ]
}
"#;
        let r: Schema = r.parse().unwrap();
        let w: Schema = w.parse().unwrap();
        let err_str = if let Result::Err(AvroError::ResolveSchema(SchemaResolutionError(s))) =
            resolve_schemas(&w, &r)
        {
            s
        } else {
            panic!("Expected schema resolution failure");
        };
        // The field name here must NOT contain `com.materialize.foo`,
        // because explicitly named types are all relative to a global
        // namespace (i.e., they don't nest).
        assert_eq!(&err_str, "Writer schema has type `Double`, but reader schema has type `Int` for field `com.materialize.bar.f1_1`");
    }

    #[test]
    fn test_extra_fields_without_default_error() {
        let r = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": "int"},
        {"name": "f2", "type": "int"}
    ]
}
"#;
        let w = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": "int"}
    ]
}
"#;
        let r: Schema = r.parse().unwrap();
        let w: Schema = w.parse().unwrap();
        let err_str = if let Result::Err(AvroError::ResolveSchema(SchemaResolutionError(s))) =
            resolve_schemas(&w, &r)
        {
            s
        } else {
            panic!("Expected schema resolution failure");
        };
        assert_eq!(
            &err_str,
            "Reader field `com.materialize.foo.f2` not found in writer, and has no default"
        );
    }

    #[test]
    fn test_duplicate_field_error() {
        let r = r#"
{
    "type": "record",
    "name": "com.materialize.bar",
    "fields": [
        {"name": "f1", "type": "int"},
        {"name": "f1", "type": "int"}
    ]
}
"#;
        let w = r#"
{
    "type": "record",
    "name": "com.materialize.bar",
    "fields": [
        {"name": "f1", "type": "int"}
    ]
}
"#;
        let r: Schema = r.parse().unwrap();
        let w: Schema = w.parse().unwrap();
        let err_str = if let Result::Err(AvroError::ResolveSchema(SchemaResolutionError(s))) =
            resolve_schemas(&w, &r)
        {
            s
        } else {
            panic!("Expected schema resolution failure");
        };
        assert_eq!(
            &err_str,
            "Duplicate field `com.materialize.bar.f1` in schema"
        );
    }

    #[test]
    fn test_decimal_field_mismatch_error() {
        let r = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}}
    ]
}
"#;
        let w = r#"
{
    "type": "record",
    "name": "com.materialize.foo",
    "fields": [
        {"name": "f1", "type": {"type": "bytes", "logicalType": "decimal", "precision": 5, "scale": 1}}
    ]
}
"#;
        let r: Schema = r.parse().unwrap();
        let w: Schema = w.parse().unwrap();
        let err_str = if let Result::Err(AvroError::ResolveSchema(SchemaResolutionError(s))) =
            resolve_schemas(&w, &r)
        {
            s
        } else {
            panic!("Expected schema resolution failure");
        };
        assert_eq!(
            &err_str,
            "Decimal types must match in precision, scale, and fixed size. Got (5, 1, None); (4, 2. None) for field `com.materialize.foo.f1`"
        );
    }
}
