// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package io.materialize;

import io.confluent.connect.avro.AvroData;
import io.debezium.config.Configuration;
import io.debezium.config.Configuration.Builder;
import io.debezium.embedded.EmbeddedEngine;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Log tables to <a href="https://avro.apache.org/docs/1.8.2/spec.html#Object+Container+Files">
 * Avro Object Container Files</a>
 */
public class Binlogger implements Consumer<SourceRecord> {
    static FileOutputStream fos;
    // The directory all avro data will go into
    String logDir;

    static Map<String, Output> schemaLogs = new HashMap<>();
    static AvroData avroDataConverter = new AvroData(50);

    public Binlogger(String logDir) {
        if (logDir == null) {
            throw new RuntimeException("log dir is required");
        }
        this.logDir = logDir;
        File logDir_ = new File(this.logDir);

        if (!logDir_.exists()) {
            System.out.printf("creating %s\n", logDir_.getAbsolutePath());
            if (!logDir_.mkdirs()) {
                throw new RuntimeException("ERROR: unable to create directory " + logDir);
            }
        } else {
            System.out.printf("all logs will go to %s\n", logDir_.getAbsolutePath());
        }
    }

    public void accept(SourceRecord s) {
        try {
            String keyName = s.topic();
            Output<Object> output = schemaLogs.get(keyName);
            if (output == null) {
                File outFile = new File(logDir + "/" + keyName);
                Schema kafkaSchema = s.valueSchema();
                org.apache.avro.Schema schema = avroDataConverter.fromConnectSchema(kafkaSchema);
                output = new Output<>(outFile, schema);
                schemaLogs.put(keyName, output);
            }
            // TODO: it seems like it should be possible not create the intermediate object, or at least
            //  to cache the schema parsing
            Object serializable = avroDataConverter.fromConnectData(s.valueSchema(), s.value());
            output.fileWriter.append(serializable);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, SQLException {
        BasicConfigurator.configure();

        ArgumentParser parser = ArgumentParsers.newFor("Binlogger").build().defaultHelp(true)
                .description("Streaming CDC out of database binlogs (currently supported: mysql and postgres");
        parser.addArgument("-t", "--type").choices("mysql", "postgres").setDefault("postgres")
                .help("Specify which database to binlog");
        parser.addArgument("-p", "--port").help("Database port").setDefault("5432");
        parser.addArgument("-H", "--hostname").help("Database hostname").setDefault("localhost");
        parser.addArgument("-d", "--database").help("Database").setDefault("postgres");
        parser.addArgument("-u", "--user").help("User").setDefault("postgres");
        parser.addArgument("-P", "--password").help("Database password").setDefault("postgres");
        parser.addArgument("--dir").help("Directory to output all serialized data to").setDefault(".");
        parser.addArgument("-S", "--save-file").help("file to keep current replication status in").setDefault("tb");
        parser.addArgument("--replication-slot")
                .help("The postgres replication slot to use, must be distinct across multiple instances of tb")
                .setDefault("tb");
        parser.addArgument("--whitelist").help(
                "A csv-separated list of tables to monitor, like so: " +
                "--whitelist schemaName1.databaseName1,schemaName2.databaseName2");
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
            return; // clean "possibly uninitialized variable" errors
        }

        String type = getNsString(ns, "type");
        String logDir = getNsString(ns, "dir");

        Builder b = Configuration.create()
            .with("database.hostname", getNsString(ns, "hostname"))
            .with("database.port", getNsString(ns, "port"))
            .with("database.user", getNsString(ns, "user"))
            .with("database.dbname", getNsString(ns, "database"))
            .with("database.password", getNsString(ns, "password"))
            .with("database.server.name", "tb")
            // Need a distinct pg_replication_slots name, "debezium" is already taken via
            // standard Materialize setup.
            .with("slot.name", getNsString(ns, "replication_slot"))
            .with("plugin.name", "pgoutput")
            // TODO: we are writing to these files but the don't seem to be having an effect
            .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
            .with("offset.storage.file.filename", getNsString(ns, "save_file") + ".offsets")
            .with("offset.flush.interval.ms", 100)
            .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
            .with("database.history.file.filename", getNsString(ns, "save_file") + ".history")
            .with("provide.transaction.metadata", true)
            .with("provide.transaction.metadata.file.filename", getNsString(ns, "save_file") + ".trx");

        String whiteListField = ns.getString("whitelist");
        if (whiteListField != null) {
            b = b.with("table.whitelist", whiteListField);
        }

        if (type.equals("mysql")) {
            b = b.with("connector.class", "io.debezium.connector.mysql.MySqlConnector").with("name",
                    "my-sql-connector");
        } else if (type.equals("postgres")) {
            b = b.with("connector.class", "io.debezium.connector.postgresql.PostgresConnector").with("name",
                    "postgres-connector");
        } else {
            System.out.printf("ERROR: unknown database type: %s\n", type);
            System.exit(1);
            return;
        }

        Configuration config = b.build();
        Binlogger bl = new Binlogger(logDir);

        // Create the engine with this configuration ...
        EmbeddedEngine engine = EmbeddedEngine.create().using(config).notifying(bl::accept).build();
        try {
            engine.run();
        } finally {
            fos.close();
        }
    }


    private static String getNsString(Namespace ns, String field) {
        String obj = ns.getString(field);
        if (obj == null) {
            throw new RuntimeException("Argument field is null: " + field);
        }
        return obj;
    }

    /**
     * Helper class that knows where it's writing to, and what its schema is
     */
    private static class Output<D> {
        File out;
        org.apache.avro.Schema schema;
        DataFileWriter<D> fileWriter;

        Output(File out, org.apache.avro.Schema schema) throws IOException {
            this.out = out;
            this.schema = schema;
            DatumWriter<D> dw = new GenericDatumWriter<>(schema);
            DataFileWriter<D> dfw = new DataFileWriter<>(dw);
            if (out.exists()) {
                System.out.printf("Appending to existing binlog %s\n", out);
                // TODO: figure out why the debezium config is not continuing from the offset it's recording
                // this.fileWriter = dfw.appendTo(out);
                // return;
                throw new RuntimeException("Cannot restart from existing: " + out);
            }
            System.out.printf("Creating new binlog %s\n", out);
            this.fileWriter = dfw.create(schema, out);
        }

        @Override
        public String toString() {
            return "Output{" +
                    "out=" + out +
                    ", schema=" + schema +
                    '}';
        }
    }

}
