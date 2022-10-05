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

package io.materialize.tb;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Command-line interface for the "tail-binlog" (TB) program.
 */
public class TB {
    private static final Logger logger = LoggerFactory.getLogger(TB.class);

    public static void main(String[] args) throws Exception {
        // Configure logging.
        BasicConfigurator.configure();

        // Parse arguments.
        ArgumentParser parser = ArgumentParsers.newFor("tb")
            .fromFilePrefix("@").build().defaultHelp(true)
            .description("Streaming CDC out of database binlogs (currently supported: mysql and postgres");
        parser.addArgument("-t", "--type").choices("mysql", "postgres").setDefault("postgres")
            .help("Specify which database to binlog");
        parser.addArgument("-p", "--port").help("Database port").setDefault("5432");
        parser.addArgument("-H", "--hostname").help("Database hostname").setDefault("localhost");
        parser.addArgument("-d", "--database").help("Database").setDefault("postgres");
        parser.addArgument("-u", "--user").help("User").setDefault("postgres");
        parser.addArgument("-P", "--password").help("Database password").setDefault("postgres");
        parser.addArgument("--dir").help("Directory to output all serialized data to").setDefault(".");
        parser.addArgument("-S", "--save-file").help("File to keep current replication status in").setDefault("tb");
        parser.addArgument("--replication-slot")
            .help("The postgres replication slot to use, must be distinct across multiple instances of tb")
            .setDefault("tb");
        parser.addArgument("--whitelist")
            .help("A csv-separated list of tables to monitor, like so: " +
                "--whitelist schemaName1.databaseName1,schemaName2.databaseName2");
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
            return;
        }

        // Build Debezium configuration.

        Properties config = new Properties();
        config.setProperty("database.hostname", ns.getString("hostname"));
        config.setProperty("database.port", ns.getString("port"));
        config.setProperty("database.user", ns.getString("user"));
        config.setProperty("database.dbname", ns.getString("database"));
        config.setProperty("database.password", ns.getString("password"));
        config.setProperty("database.server.name", "tb");
        config.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");

        config.setProperty("database.history.file.filename", ns.getString("save_file") + ".history");
        config.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        config.setProperty("offset.storage.file.filename", ns.getString("save_file") + ".offsets");
        config.setProperty("offset.flush.interval.ms", "100");
        config.setProperty("plugin.name", "pgoutput");
        config.setProperty("provide.transaction.metadata", "true");
        config.setProperty("provide.transaction.metadata.file.filename", ns.getString("save_file") + ".trx");

        // Need a distinct pg_replication_slots name, as "debezium" is already
        // taken by the standard Materialize setup.
        config.setProperty("slot.name", ns.getString("replication_slot"));

        String whiteListField = ns.getString("whitelist");
        if (whiteListField != null) {
            config.setProperty("table.whitelist", whiteListField);
        }

        String type = ns.getString("type");
        if (type.equals("mysql")) {
            config.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
            config.setProperty("name", "mysql-connector");
            config.setProperty("database.allowPublicKeyRetrieval", "true"); // required for MySql8 if connection does not have SSL
        } else if (type.equals("postgres")) {
            config.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
            config.setProperty("name", "postgres-connector");
        } else {
            // The value of `type` has been validated by the argument parser.
            throw new RuntimeException("unreachable: invalid type");
        }

        Path dir = Paths.get(ns.getString("dir"));
        run(config, dir);
    }

    private static void run(Properties config, Path dir) throws Exception {
        Files.createDirectories(dir);
        logger.info("Storing data files in {}", dir);

        ChangeWriter cw = new ChangeWriter(dir);
        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine
            .create(ChangeEventFormat.of(Connect.class))
            .using(config)
            .notifying(cw)
            .build();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        // Schedule background flush task. Exceptions in this task can indicate
        // disk corruption, and so we use an ExplodingRunnable to crash the
        // process loudly if one occurs! scheduleWithFixedDelay would otherwise
        // silently suppress the exception.
        executor.scheduleWithFixedDelay(
            new ExplodingRunnable(() -> cw.flushAll()),
            0, 1, TimeUnit.SECONDS
        );

        // Run Debezium. If the task ever returns, something has gone wrong,
        // so exit the process with a failing exit code. Debezium should have
        // already logged an error message.
        executor.submit(engine).get();
        System.exit(1);
    }
}
