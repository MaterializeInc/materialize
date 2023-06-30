// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

use bytes::{Buf, Bytes};
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use mz_ore::now::NowFn;
use mz_repr::{Datum, RelationDesc, Row, RowArena};
use mz_storage_types::sources::{Generator, MzOffset, LDBC_DESCS, LDBC_PERSON_KNOWS_PERSON_OUTPUT};
use timely::dataflow::operators::to_stream::Event;
use tracing::error;

#[derive(Clone, Debug)]
pub struct Ldbc {
    pub urls: Vec<String>,
}

impl Generator for Ldbc {
    fn by_seed(
        &self,
        _: NowFn,
        _seed: Option<u64>,
        _resume_offset: MzOffset,
    ) -> Box<(dyn Iterator<Item = (usize, Event<Option<MzOffset>, (Row, i64)>)>)> {
        let bytes = match fetch_urls_compressed(&self.urls) {
            Ok(bytes) => bytes,
            Err(err) => {
                error!("could not download file: {err}");
                return Box::new(std::iter::empty());
            }
        };
        let mut archive = tar::Archive::new(bytes.as_slice());
        let entries = match archive.entries() {
            Ok(entries) => entries,
            Err(err) => {
                error!("count not extract tar: {err}");
                return Box::new(std::iter::empty());
            }
        };
        let entries = entries
            .map(|entry| {
                let mut entry = entry.unwrap();
                let path = entry.path().unwrap().into_owned();
                let mut bytes = Vec::new();
                entry.read_to_end(&mut bytes).unwrap();
                (path, bytes)
            })
            .collect::<BTreeMap<_, _>>();
        let ctx = Context {
            entries: entries.into_iter(),
            rdr: None,
            record: StringRecord::new(),
            arena: RowArena::new(),
            offset: 0.into(),
            pending: None,
        };
        Box::new(ctx)
    }
}

struct CsvFile {
    rdr: csv::Reader<bytes::buf::Reader<bytes::Bytes>>,
    desc: &'static RelationDesc,
    output: usize,
}

struct Context {
    entries: std::collections::btree_map::IntoIter<PathBuf, Vec<u8>>,
    rdr: Option<CsvFile>,
    record: StringRecord,
    arena: RowArena,
    offset: MzOffset,
    pending: Option<(usize, Row)>,
}

impl Context {
    /// Opens the next file if `self.rdr` is `None`. `self.rdr` is set to `None` if there are no
    /// more files to read.
    fn next_file(&mut self) {
        while self.rdr.is_none() {
            let Some((mut path, mut entry)) = self.entries.next() else {
                return;
            };
            // Gunzip gz files, removing the .gz extension.
            if matches!(path.extension().and_then(|s| s.to_str()), Some("gz")) {
                let mut buf = Vec::new();
                if let Err(err) = GzDecoder::new(entry.as_slice()).read_to_end(&mut buf) {
                    error!("error gunzipping inner ldbc file {:?}: {}", path, err);
                    continue;
                }
                path = path.with_extension("");
                entry = buf;
            }
            // Skip non-CSV files.
            if !matches!(path.extension().and_then(|s| s.to_str()), Some("csv")) {
                continue;
            }
            let parts = path.parent().and_then(|path| {
                let mut parts = path.components().rev();
                let Some(entity) = parts.next() else {
                    return None;
                };
                let Some(entity) = entity.as_os_str().to_str() else {
                    return None;
                };
                let Some(load) = parts.next() else {
                    return None;
                };
                let Some(load) = load.as_os_str().to_str() else {
                    return None;
                };
                Some((load, entity))
            });
            let (output, desc) = match parts {
                Some(("static" | "dynamic", entity)) => {
                    let entity = entity.to_lowercase();
                    let Some((output, desc)) = LDBC_DESCS.get(entity.as_str()) else {
                        error!("could not find ldbc relation: {entity}");
                        continue;
                    };
                    (output, desc)
                }
                Some(_) => continue,
                None => continue,
            };
            let reader = Bytes::from(entry).reader();
            let rdr = ReaderBuilder::new().delimiter(b'|').from_reader(reader);
            self.rdr = Some(CsvFile {
                rdr,
                desc,
                output: *output,
            });
        }
    }

    /// Reads the next record out of `self.rdr`. Returns `None` if `self.rdr` is out of records.
    fn next_record(&mut self) -> Option<(usize, Row)> {
        self.rdr
            .as_mut()
            .and_then(|csv| match csv.rdr.read_record(&mut self.record) {
                Ok(true) => {
                    let row = Row::pack(self.record.iter().zip(csv.desc.iter_types()).map(
                        |(field, typ)| {
                            let pgtyp = mz_pgrepr::Type::from(&typ.scalar_type);
                            match mz_pgrepr::Value::decode_text(&pgtyp, field.as_bytes()) {
                                Ok(value) => value.into_datum(&self.arena, &pgtyp),
                                Err(_) => {
                                    if field.is_empty() && typ.nullable {
                                       return Datum::Null;
                                    }
                                  error!("could not decode typ {typ:?} for value '{field}' in ldbc output {}", csv.output);
                                    panic!("could not decode typ {typ:?} for value '{field}' in ldbc output {}", csv.output);
                                }
                            }
                        },
                    ));
                    // Make symmetric.
                    if csv.output == LDBC_PERSON_KNOWS_PERSON_OUTPUT {
                        assert!(self.pending.is_none());
                        let mut iter = row.iter();
                        let creationdate = iter.next().expect("must exist");
                        let person1id = iter.next().expect("must exist");
                        let person2id = iter.next().expect("must exist");
                        let swapped = Row::pack_slice(&[
                            creationdate,
                            person2id,
                            person1id,
                        ]);
                        self.pending = Some((csv.output, swapped));
                    }
                    Some((csv.output, row))
                }
                Ok(false) => None,
                Err(_) => None,
            })
    }
}

impl Iterator for Context {
    type Item = (usize, Event<Option<MzOffset>, (Row, i64)>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((output, row)) = self.pending.take() {
            self.offset += 1;
            return Some((output, Event::Message(self.offset, (row, 1))));
        }
        loop {
            match self.next_record() {
                Some((output, row)) => {
                    self.offset += 1;
                    return Some((output, Event::Message(self.offset, (row, 1))));
                }
                None => {
                    // If we're out of records for this file, close the current file and its data.
                    if let Some(csv) = self.rdr.take() {
                        return Some((csv.output, Event::Progress(Some(self.offset))));
                    }
                    // Otherwise open the next file.
                    self.next_file();
                    // No more files.
                    if self.rdr.is_none() {
                        return None;
                    }
                    continue;
                }
            }
        }
    }
}

/// Fetches URLs whose bodies are zstd compressed and returns their response body uncompressed and
/// concatenated.
fn fetch_urls_compressed(urls: &[String]) -> Result<Vec<u8>, anyhow::Error> {
    let mut buf = Vec::new();
    for url in urls {
        let resp = reqwest::blocking::get(url)?.error_for_status()?;
        zstd::stream::copy_decode(resp, &mut buf)?;
    }
    Ok(buf)
}
