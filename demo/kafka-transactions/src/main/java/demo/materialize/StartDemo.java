// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package demo.materialize;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;

//TODO(ncrooks): configure demo to work on multiple topics

public class StartDemo {

    /**
     * Registers command line options
     *
     * @return option datastructure
     */
    private static Options createOptions() {
        Options options = new Options();

        Option optSchema = Option.builder("r")
            .argName("Schema Registry URL")
            .longOpt("schema-registry")
            .desc("Default: localhost:8081")
            .hasArg()
            .build();

        Option optBootstrap = Option.builder("b")
            .argName("Bootstrap Servers")
            .longOpt("bootstrap-servers")
            .desc("Default: localhost:9092")
            .hasArg()
            .build();

        Option optPartitionCount = Option.builder("p")
            .argName("Partition Count")
            .longOpt("partition-count")
            .desc("Default: 1")
            .hasArg()
            .build();

        Option optWritesPerPartition = Option.builder("w")
            .argName("Writes per Partition")
            .longOpt("partition-writes")
            .desc("Default: 1")
            .hasArg()
            .build();

        Option optAbortRate = Option.builder("a")
            .argName("Producer Abort Rate")
            .longOpt("producer-abort")
            .desc("Default: 0")
            .hasArg()
            .build();

        Option optKeySize = Option.builder("k")
            .argName("Key Size")
            .longOpt("key-size")
            .desc("Default: 100")
            .hasArg()
            .build();

        Option optPayloadSize = Option.builder("d")
            .argName("Payload Size")
            .longOpt("payload-size")
            .desc("Default: 100")
            .hasArg()
            .build();

        Option optOperationCount = Option.builder("o")
            .argName("Operation Count")
            .longOpt("operation-count")
            .desc("Default: 100000")
            .hasArg()
            .build();

        Option optTransactionMode = Option.builder("t")
            .argName("Transaction Mode")
            .longOpt("transactional")
            .desc("Default: true")
            .hasArg()
            .build();


        Option optTopicName = Option.builder("n")
            .argName("Topic Names")
            .longOpt("topic-names")
            .desc("MUST SPECIFY - Name of the Kafka topic (Maximum 10)")
            .required(true)
            .numberOfArgs(Option.UNLIMITED_VALUES)
            .valueSeparator(',')
            .build();

        Option optTransactionId = Option.builder("j")
            .argName("Producer Transaction Id")
            .longOpt("producer-id")
            .desc("MUST SPECIFY - This is used to identify the same producer instance across process restarts")
            .required(true)
            .hasArg()
            .build();

        Option optReplicationFactor = Option.builder("r")
            .argName("Topic Replication Factor")
            .longOpt("replication-factor")
            .desc("The replication factor of a topic can be no greater than the current number of brokers")
            .hasArg()
            .build();

        Option optProducerCount = Option.builder("c")
            .argName("Producers")
            .longOpt("producer-count")
            .desc("Number of concurrent producers")
            .hasArg()
            .build();

        options.addOption(optSchema);
        options.addOption(optBootstrap);
        options.addOption(optPartitionCount);
        options.addOption(optWritesPerPartition);
        options.addOption(optAbortRate);
        options.addOption(optKeySize);
        options.addOption(optPayloadSize);
        options.addOption(optOperationCount);
        options.addOption(optTransactionMode);
        options.addOption(optTopicName);
        options.addOption(optTransactionId);
        options.addOption(optReplicationFactor);
        options.addOption(optProducerCount);

        return options;
    }

    /**
     * Generates the corresponding configuration from the command line
     * options supplied
     *
     * @param cmd - the parsed command line arguments
     * @return the configuration corresponding to the arguments provided (or defaults)
     */
    private static Config createConfig(CommandLine cmd) {

        String schemaRegistryUrl = cmd.getOptionValue("r", "localhost:8081");
        String bootstrapServer = cmd.getOptionValue("b", "localhost:9092");
        List<String> topicNames = Arrays.asList(cmd.getOptionValues('n'));
        int partitionCount = Integer.parseInt(cmd.getOptionValue("p", "1"));
        int writesPerPartition = Integer.parseInt(cmd.getOptionValue("w", "1"));
        int abortRate = Integer.parseInt(cmd.getOptionValue("a", "0"));
        int keySize = Integer.parseInt(cmd.getOptionValue("k", "100"));
        int payloadSize = Integer.parseInt(cmd.getOptionValue("p", "100"));
        int operationCount = Integer.parseInt(cmd.getOptionValue("o", "1000000"));
        boolean transactionMode = Boolean.parseBoolean(cmd.getOptionValue("t", "true"));
        int transactionId = Integer.parseInt(cmd.getOptionValue("j"));
        short replicationFactor = Short.parseShort(cmd.getOptionValue("r", "1"));
        int nbProducers = Integer.parseInt(cmd.getOptionValue("c", "1"));

        return new Config(schemaRegistryUrl, bootstrapServer, topicNames, partitionCount, writesPerPartition, abortRate,
            keySize, payloadSize, operationCount, transactionMode, transactionId, replicationFactor, nbProducers);
    }

    public static void main(String[] args) {
        Options options = createOptions();
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            final Config config = createConfig(cmd);
            System.out.format("Demo Configuration: \n \n Schema Registry URL: %s \n Bootstrap Server %s \n  Topics %s \n Partition Count %d \n " +
                    "Writes Per Partition %d \n Abort Rate %d \n Key Size %d \n Payload Size %d \n Operation Count %d \n Transaction Mode %b \n Transaction ID %d \n",
                config.getSchemaRegistryUrl(), config.getBootstrapServers(), Arrays.toString(config.getTopics().toArray()), config.getPartitionCount(),
                config.getWritesPerPartition(), config.getAbortRate(), config.getKeySize(), config.getPayloadSize(), config.getOperationCount(),
                config.isTransactional(), config.getTransactionId(), config.getNbProducers());
            config.createTopics();
            System.out.println("Starting data loading ...");

            ExecutorService service = Executors.newCachedThreadPool();
            LinkedList<Future<Integer>> producer_tasks = new LinkedList<Future<Integer>>();
            for (int i = 0; i < config.getNbProducers(); i++) {
                final int offset = i;
                Callable c = () -> {
                    TransactionalProducer producer = new TransactionalProducer(config, offset);
                    return producer.startGeneratingData();
                };
                Future<Integer> result = service.submit(c);
                producer_tasks.add(result);
            }
            int total_ops = 0;
            for (Future<Integer> result: producer_tasks) {
                total_ops+=result.get();
            }
            System.out.format("Finished loading data. Expected result: %d  operations. Exiting ... \n", total_ops);
        } catch (ParseException | InterruptedException | ExecutionException e) {
            System.err.println("Error parsing command line options");
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("TrxKafkaDemo", "", options, "", true);
            System.exit(1);
        }

    }
}
