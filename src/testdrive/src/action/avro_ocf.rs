// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::path;

use avro::Writer;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct WriteAction {
    path: String,
    schema: String,
    records: Vec<String>,
}

pub fn build_write(mut cmd: BuiltinCommand) -> Result<WriteAction, String> {
    let path = cmd.args.string("path")?;
    let schema = cmd.args.string("schema")?;
    let records = cmd.input;
    cmd.args.done()?;
    if path.contains(path::MAIN_SEPARATOR) {
        // The goal isn't security, but preventing mistakes.
        return Err("separators in paths are forbidden".into());
    }
    Ok(WriteAction {
        path,
        schema,
        records,
    })
}

impl Action for WriteAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        // Files are written to a fresh temporary directory, so no need to
        // explicitly remove the file here.
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let path = state.temp_dir.path().join(&self.path);
        println!("writing to {}", path.display());
        let mut file = File::create(path).map_err(|e| e.to_string())?;
        let schema = interchange::avro::parse_schema(&self.schema)
            .map_err(|e| format!("parsing avro schema: {}", e))?;
        let mut writer = Writer::new(schema.clone(), &mut file);
        for record in &self.records {
            let record = crate::format::avro::json_to_avro(
                &serde_json::from_str(record).map_err(|e| format!("parsing avro datum: {}", e))?,
                schema.top_node(),
            )?;
            writer
                .append(record)
                .map_err(|e| format!("writing avro record: {}", e))?;
        }
        writer
            .flush()
            .map_err(|e| format!("flushing avro writer: {}", e))?;
        file.sync_all()
            .map_err(|e| format!("error syncing file: {}", e))?;
        Ok(())
    }
}
