// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic handling reading from Avro format at user level.
use std::io::ErrorKind;
use std::str::{from_utf8, FromStr};

use failure::Error;
use serde_json::from_slice;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::decode::decode;
use crate::schema::{
    resolve_schemas, FullName, NamedSchemaPiece, ParseSchemaError, RecordField,
    ResolvedDefaultValueField, SchemaNodeOrNamed, SchemaPiece, SchemaPieceOrNamed,
    SchemaPieceRefOrNamed,
};
use crate::schema::{ResolvedRecordField, Schema};
use crate::types::Value;
use crate::util::{self, DecodeError};
use crate::{Codec, SchemaResolutionError};
use futures::stream::try_unfold;
use futures::Stream;

use sha2::Sha256;
use std::collections::HashMap;

// Internal Block reader.
#[derive(Debug, Clone)]
struct Block<R> {
    reader: R,
    // Internal buffering to reduce allocation.
    buf: Vec<u8>,
    buf_idx: usize,
    // Number of elements expected to exist within this block.
    message_count: usize,
    marker: [u8; 16],
    codec: Codec,
    writer_schema: Schema,
    resolved_schema: Option<Schema>,
}

impl<R: AsyncRead + Unpin + Send> Block<R> {
    async fn new(reader: R, reader_schema: Option<&Schema>) -> Result<Block<R>, Error> {
        let mut block = Block {
            reader,
            codec: Codec::Null,
            writer_schema: Schema {
                named: vec![],
                indices: Default::default(),
                top: SchemaPieceOrNamed::Piece(SchemaPiece::Null),
            },
            buf: vec![],
            buf_idx: 0,
            message_count: 0,
            marker: [0; 16],
            resolved_schema: None,
        };

        block.read_header(reader_schema).await?;
        Ok(block)
    }

    fn schema(&self) -> &Schema {
        self.resolved_schema.as_ref().unwrap_or(&self.writer_schema)
    }

    /// Try to read the header and to set the writer `Schema`, the `Codec` and the marker based on
    /// its content.
    async fn read_header(&mut self, reader_schema: Option<&Schema>) -> Result<(), Error> {
        let meta_schema = Schema {
            named: vec![],
            indices: Default::default(),
            top: SchemaPiece::Map(Box::new(SchemaPiece::Bytes.into())).into(),
        };

        let mut buf = [0u8; 4];
        self.reader.read_exact(&mut buf).await?;

        if buf != [b'O', b'b', b'j', 1u8] {
            return Err(DecodeError::new("wrong magic in header").into());
        }

        if let Value::Map(meta) = decode(meta_schema.top_node(), &mut self.reader).await? {
            // TODO: surface original parse schema errors instead of coalescing them here
            let json = meta
                .get("avro.schema")
                .ok_or_else(|| DecodeError::new("unable to parse schema: 'avro.schema' missing"))
                .and_then(|bytes| {
                    if let Value::Bytes(ref bytes) = *bytes {
                        from_slice(bytes.as_ref()).map_err(|e| {
                            DecodeError::new(format!("unable to decode schema bytes: {}", e))
                        })
                    } else {
                        Err(DecodeError::new(format!(
                            "unable to decode schema: expected Bytes, got: {:?}",
                            bytes
                        )))
                    }
                })?;
            let schema = Schema::parse(&json).map_err(|e| {
                ParseSchemaError::new(format!("unable to parse json as avro schema: {}", e))
            })?;

            self.writer_schema = schema;

            if let Some(reader_schema) = reader_schema {
                if reader_schema.fingerprint::<Sha256>().bytes
                    != self.writer_schema.fingerprint::<Sha256>().bytes
                {
                    self.resolved_schema =
                        Some(resolve_schemas(&self.writer_schema, reader_schema)?)
                }
            }

            self.codec = match meta.get("avro.codec") {
                Some(Value::Bytes(ref bytes)) => from_utf8(bytes.as_ref())
                    .map_err(|e| DecodeError::new(format!("unable to decode codec: {}", e)))
                    .and_then(|codec| {
                        Codec::from_str(codec).map_err(|_| {
                            DecodeError::new(format!("unrecognized codec '{}'", codec))
                        })
                    }),
                Some(codec) => Err(DecodeError::new(format!(
                    "unable to parse codec: expected bytes, got: {:?}",
                    codec
                ))),
                None => Ok(Codec::Null),
            }?;
        } else {
            return Err(DecodeError::new("no metadata in header").into());
        }

        let mut buf = [0u8; 16];
        self.reader.read_exact(&mut buf).await?;
        self.marker = buf;

        Ok(())
    }

    async fn fill_buf(&mut self, n: usize) -> Result<(), Error> {
        // We don't have enough space in the buffer, need to grow it.
        if n >= self.buf.capacity() {
            self.buf.reserve(n);
        }

        unsafe {
            self.buf.set_len(n);
        }
        self.reader.read_exact(&mut self.buf[..n]).await?;
        self.buf_idx = 0;
        Ok(())
    }

    /// Try to read a data block. The objects are stored in an internal buffer to the `Reader`.
    async fn read_block_next(&mut self) -> Result<(), Error> {
        assert!(self.is_empty(), "Expected self to be empty!");
        match util::read_long(&mut self.reader).await {
            Ok(block_len) => {
                self.message_count = block_len as usize;
                let block_bytes = util::read_long(&mut self.reader).await?;
                self.fill_buf(block_bytes as usize).await?;
                let mut marker = [0u8; 16];
                self.reader.read_exact(&mut marker).await?;

                if marker != self.marker {
                    return Err(
                        DecodeError::new("block marker does not match header marker").into(),
                    );
                }

                // NOTE (JAB): This doesn't fit this Reader pattern very well.
                // `self.buf` is a growable buffer that is reused as the reader is iterated.
                // For non `Codec::Null` variants, `decompress` will allocate a new `Vec`
                // and replace `buf` with the new one, instead of reusing the same buffer.
                // We can address this by using some "limited read" type to decode directly
                // into the buffer. But this is fine, for now.
                self.codec.decompress(&mut self.buf)?;

                return Ok(());
            }
            Err(e) => {
                if let ErrorKind::UnexpectedEof = e.downcast::<::std::io::Error>()?.kind() {
                    // to not return any error in case we only finished to read cleanly from the stream
                    return Ok(());
                }
            }
        };
        Err(DecodeError::new("unable to read block").into())
    }

    fn len(&self) -> usize {
        self.message_count
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    async fn read_next(&mut self) -> Result<Option<Value>, Error> {
        if self.is_empty() {
            self.read_block_next().await?;
            if self.is_empty() {
                return Ok(None);
            }
        }

        let mut block_bytes = &self.buf[self.buf_idx..];
        let b_original = block_bytes.len();
        //
        // ```
        // let item = from_avro_datum(self.schema(), &mut block_bytes).await?;
        // ```
        // spews pages of incomprehensible Send/Sync errors, but splitting it into
        // two statements works...
        let schema = self.schema();
        let item = from_avro_datum(schema, &mut block_bytes).await?;
        self.buf_idx += b_original - block_bytes.len();
        self.message_count -= 1;
        Ok(Some(item))
    }
}

pub struct Reader<R> {
    block: Block<R>,
}

impl<R: AsyncRead + Unpin + Send> Reader<R> {
    /// Creates a `Reader` given something implementing the `tokio::io::AsyncRead` trait to read from.
    /// No reader `Schema` will be set.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub async fn new(reader: R) -> Result<Reader<R>, Error> {
        let block = Block::new(reader, None).await?;
        let reader = Reader { block };
        Ok(reader)
    }

    /// Creates a `Reader` given a reader `Schema` and something implementing the `tokio::io::AsyncRead` trait
    /// to read from.
    ///
    /// **NOTE** The avro header is going to be read automatically upon creation of the `Reader`.
    pub async fn with_schema(schema: &Schema, reader: R) -> Result<Reader<R>, Error> {
        let block = Block::new(reader, Some(schema)).await?;
        Ok(Reader { block })
    }

    /// Get a reference to the writer `Schema`.
    pub fn writer_schema(&self) -> &Schema {
        &self.block.writer_schema
    }

    #[inline]
    /// Read the next Avro value from the file, if one exists.
    pub async fn read_next(&mut self) -> Result<Option<Value>, Error> {
        self.block.read_next().await
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<Value, Error>> + Unpin {
        Box::pin(try_unfold(self, |mut r| async {
            Ok(r.read_next().await?.map(|v| (v, r)))
        }))
    }
}

pub struct SchemaResolver<'a> {
    pub named: Vec<Option<NamedSchemaPiece>>,
    pub indices: HashMap<FullName, usize>,
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
    ) -> Result<SchemaPiece, Error> {
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
                                        "Reader field {} not found in writer, and has no default",
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
                                    "Duplicate field {} in schema",
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
                            let new_inner = self.resolve(w_node, r_node)?;
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
                        if r_map.contains_key(&s) {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                SchemaPiece::ResolveEnum {
                    doc: doc.clone(),
                    symbols,
                }
            }
            (SchemaPiece::Fixed { size: wsz }, SchemaPiece::Fixed { size: rsz }) => {
                if *wsz == *rsz {
                    SchemaPiece::Fixed { size: *wsz }
                } else {
                    return Err(SchemaResolutionError::new(format!(
                        "Fixed schema {:?}: sizes don't match ({}, {})",
                        &rs.name, wsz, rsz
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
                        "Decimal schema {:?}: precisions don't match: {}, {}",
                        &rs.name, wp, rp
                    ))
                    .into());
                }
                if wscale != rscale {
                    return Err(SchemaResolutionError::new(format!(
                        "Decimal schema {:?}: sizes don't match: {}, {}",
                        &rs.name, wscale, rscale
                    ))
                    .into());
                }
                if wsz != rsz {
                    return Err(SchemaResolutionError::new(format!(
                        "Decimal schema {:?}: sizes don't match: {:?}, {:?}",
                        &rs.name, wsz, rsz
                    ))
                    .into());
                }
                SchemaPiece::Decimal {
                    precision: *wp,
                    scale: *wscale,
                    fixed_size: *wsz,
                }
            }
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
    ) -> Result<SchemaPieceOrNamed, Error> {
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
                // TODO (brennan) - can we avoid cloning this?
                let w2r = self.writer_to_reader_names.clone();
                let permutation = w_inner
                    .variants()
                    .iter()
                    .map(move |w_inner| {
                        r_inner
                            .resolve(w_inner, &w2r)
                            .map(|(r_idx, r_inner)| (r_idx, w_inner, r_inner))
                    })
                    .flat_map(|o| {
                        o.map(|(r_idx, w_inner, r_inner)| {
                            self.resolve(writer.step(w_inner), reader.step(r_inner))
                                .ok()
                                .map(|schema| (r_idx, schema))
                        })
                    })
                    .collect::<Vec<_>>();
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveUnionUnion { permutation })
            }
            // Writer is concrete; reader is union
            (other, SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(r_inner))) => {
                let (index, r_inner) = r_inner
                    .resolve_ref(other, &self.writer_to_reader_names)
                    .ok_or_else(|| {
                        SchemaResolutionError::new("No matching schema in union".to_string())
                    })?;
                let inner = Box::new(self.resolve(writer.step_ref(other), reader.step(r_inner))?);
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveConcreteUnion { index, inner })
            }
            // Writer is union; reader is concrete
            (SchemaPieceRefOrNamed::Piece(SchemaPiece::Union(w_inner)), other) => {
                let (index, w_inner) = w_inner
                    .resolve_ref(other, &self.reader_to_writer_names)
                    .ok_or_else(|| {
                        SchemaResolutionError::new("No matching schema in union".to_string())
                    })?;
                let inner = Box::new(self.resolve(writer.step(w_inner), reader.step_ref(other))?);
                SchemaPieceOrNamed::Piece(SchemaPiece::ResolveUnionConcrete { index, inner })
            }
            // Any other anonymous type.
            (SchemaPieceRefOrNamed::Piece(wp), SchemaPieceRefOrNamed::Piece(rp)) => {
                match (wp, rp) {
                    (SchemaPiece::Null, SchemaPiece::Null) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Null)
                    }
                    (SchemaPiece::Boolean, SchemaPiece::Boolean) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Boolean)
                    }
                    (SchemaPiece::Int, SchemaPiece::Int) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Int)
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
                    (SchemaPiece::Long, SchemaPiece::Long) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Long)
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
                    (SchemaPiece::Date, SchemaPiece::Date) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::Date)
                    }
                    (SchemaPiece::TimestampMilli, SchemaPiece::TimestampMilli) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::TimestampMilli)
                    }
                    (SchemaPiece::TimestampMicro, SchemaPiece::TimestampMicro) => {
                        SchemaPieceOrNamed::Piece(SchemaPiece::TimestampMicro)
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
                            return Err(SchemaResolutionError::new(format!("Decimal types must match in precision, scale, and fixed size. Got ({:?}, {:?}, {:?}); ({:?}, {:?}. {:?})", wp, ws, wf, rp, rs, rf)).into());
                        }
                    }
                    (ws, rs) => {
                        return Err(SchemaResolutionError::new(format!(
                            "Schemas don't match: {:?}, {:?}",
                            ws, rs
                        ))
                        .into())
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
                            self.resolve_named(writer.root, reader.root, w_index, r_index)?;
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
                    "Schemas don't match: {:?}, {:?}",
                    ws.get_piece_and_name(&writer.root).0,
                    rs.get_piece_and_name(&reader.root).0
                ))
                .into())
            }
        };
        Ok(inner)
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
pub async fn from_avro_datum<R: AsyncRead + Unpin + Send>(
    schema: &Schema,
    reader: &mut R,
) -> Result<Value, Error> {
    let value = decode(schema.top_node(), reader).await?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Record, ToAvro};
    use crate::Reader;

    use futures::stream::StreamExt;
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

    #[tokio::test]
    async fn test_from_avro_datum() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[54, 6, 102, 111, 111];

        let mut record = Record::new(schema.top_node()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");
        let expected = record.avro();

        assert_eq!(
            from_avro_datum(&schema, &mut encoded).await.unwrap(),
            expected
        );
    }

    #[tokio::test]
    async fn test_null_union() {
        let schema = Schema::parse_str(UNION_SCHEMA).unwrap();
        let mut encoded: &'static [u8] = &[2, 0];

        assert_eq!(
            from_avro_datum(&schema, &mut encoded).await.unwrap(),
            Value::Union(1, Box::new(Value::Long(0)))
        );
    }

    #[tokio::test]
    async fn test_reader_stream() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let mut reader = Reader::with_schema(&schema, ENCODED)
            .await
            .unwrap()
            .into_stream();

        let mut record1 = Record::new(schema.top_node()).unwrap();
        record1.put("a", 27i64);
        record1.put("b", "foo");

        let mut record2 = Record::new(schema.top_node()).unwrap();
        record2.put("a", 42i64);
        record2.put("b", "bar");

        let expected = vec![record1.avro(), record2.avro()];

        let mut i = 0;
        while let Some(value) = reader.next().await {
            assert_eq!(value.unwrap(), expected[i]);
            i += 1
        }
    }

    #[tokio::test]
    async fn test_reader_invalid_header() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED.to_owned().into_iter().skip(1).collect::<Vec<u8>>();
        assert!(Reader::with_schema(&schema, &invalid[..]).await.is_err());
    }

    #[tokio::test]
    async fn test_reader_invalid_block() {
        let schema = Schema::parse_str(SCHEMA).unwrap();
        let invalid = ENCODED
            .to_owned()
            .into_iter()
            .rev()
            .skip(19)
            .collect::<Vec<u8>>()
            .into_iter()
            .rev()
            .collect::<Vec<u8>>();
        let mut reader = Reader::with_schema(&schema, &invalid[..])
            .await
            .unwrap()
            .into_stream();
        while let Some(value) = reader.next().await {
            assert!(value.is_err());
        }
    }

    #[tokio::test]
    async fn test_reader_empty_buffer() {
        let empty = Cursor::new(Vec::new());
        assert!(Reader::new(empty).await.is_err());
    }

    #[tokio::test]
    async fn test_reader_only_header() {
        let invalid = ENCODED
            .to_owned()
            .into_iter()
            .take(165)
            .collect::<Vec<u8>>();
        let mut reader = Reader::new(&invalid[..]).await.unwrap().into_stream();
        while let Some(value) = reader.next().await {
            assert!(value.is_err());
        }
    }
}
