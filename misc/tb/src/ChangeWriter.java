// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package io.materialize.tb;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.avro.AvroData;
import io.debezium.engine.RecordChangeEvent;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Receives Debezium change events and writes them to Avro OCF files.
*
* The API is entirely thread-safe. It is safe to call any methods concurrently
* from multiple threads.
*/
public class ChangeWriter implements Consumer<RecordChangeEvent<SourceRecord>> {
    private static final Logger logger = LoggerFactory.getLogger(ChangeWriter.class);

    private static final int avroSchemaCacheSize = 8;

    private Path dir;
    private Map<String, DataFileWriter> schemaLogs = new HashMap();
    private AvroData avroConverter = new AvroData(avroSchemaCacheSize);

    /**
    * Constructs a new change writer.
    *
    * @param dir the directory in which to create Avro OCF files
    */
    public ChangeWriter(Path dir) {
        this.dir = dir;
    }

    /**
    * Accepts a new change event from Debezium.
    *
    * The change event will be written to an Avro OCF file named after the
    * topic that originated the change event. If that file does not already
    * exist, it is created.
    *
    * @param event the change event
    */
    public void accept(RecordChangeEvent<SourceRecord> event) {
        try {
            acceptInternal(event);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void acceptInternal(RecordChangeEvent<SourceRecord> event) throws IOException {
        SourceRecord record = event.record();

        DataFileWriter dfw = schemaLogs.get(record.topic());
        if (dfw == null) {
            Schema schema = avroConverter.fromConnectSchema(record.valueSchema());
            dfw = new DataFileWriter(new GenericDatumWriter(schema));
            schemaLogs.put(record.topic(), dfw);

            File file = this.dir.resolve(record.topic()).toFile();
            if (file.exists()) {
                logger.info("Appending to existing binlog {}", file);
                dfw.appendTo(file);
            } else {
                logger.info("Creating new binlog {}", file);
                dfw.create(schema, file);
            }
        }

        Object datum = avroConverter.fromConnectData(record.valueSchema(), record.value());
        dfw.append(datum);
    }

    /**
    * Flushes any outstanding changes across all open Avro OCF files.
    */
    public synchronized void flushAll() {
        for (Map.Entry<String, DataFileWriter> entry : schemaLogs.entrySet()) {
            try {
                entry.getValue().flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
