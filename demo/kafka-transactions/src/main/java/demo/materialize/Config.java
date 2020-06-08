// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package demo.materialize;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 * Summarises configuration parameters for demo. It includes functions to randomly generate data
 */
public class Config {

    /**
     * Schema Registry URL (HTTP)
     */
    private final String schemaRegistryUrl;

    /**
     * Bootstrap Server address
     */
    private final String bootstrapServers;

    /**
     * Topic Name
     */
    private final List<String> topicNames;

    /**
     * Number of partitions for the chosen topic
     */
    private final int partitionCount;

    /**
     * Maximum number of writes to execute per partition
     */
    private final int writesPerPartition;

    /**
     * Percentage of time a producer should abort a transaction
     */
    private final int abortRate;

    /**
     * Size of key data (bytes)
     */
    private final int keySize;

    /**
     * Size of payload data (bytes)
     */
    private final int payloadSize;

    /**
     * Number of operations to execute
     */
    private final int operationCount;

    /**
     * Transactional mode on/off: if true, wrap operations in a transaction
     */
    private final boolean transactionMode;

    /**
     * Kafka Transaction ID. This is used to identify the same producer instance across process restarts
     */
    private final int transactionId;

    /**
     * Replication factor (cannot be greater than the current number of brokers)
     */
    private final short replicationFactor;

    /**
     * Number of concurrent producers
     */
    private final int nbProducers;

    /**
     * Random generator used for generating data, aborts, etc.
     */
    private final Random ranGen;

    public Config(String schemaRegistryUrl, String bootstrapServers, List<String> topicNames, int partitionCount, int writesPerPartition,
                  int abortRate, int keySize, int payloadSize, int operationCount, boolean transactionMode, int transactionId,
                  short replicationFactor, int nbProducers) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.bootstrapServers = bootstrapServers;
        this.topicNames = topicNames;
        this.partitionCount = partitionCount;
        this.writesPerPartition = writesPerPartition;
        this.abortRate = abortRate;
        this.keySize = keySize;
        this.payloadSize = payloadSize;
        this.ranGen = new Random();
        this.operationCount = operationCount;
        this.transactionMode = transactionMode;
        this.replicationFactor = replicationFactor;
        this.transactionId = transactionId;
        this.nbProducers = nbProducers;
    }

    /**
     * Generates random payload consisting of bytes chosen with uniform probability
     * Payload will have #payloadSize
     *
     * @return the generated payload
     */
    public String generateRandomPayload() {
        return new String(generateRandomBytes(payloadSize));
    }


    /**
     * Generates random payload consisting of bytes chosen with uniform probability
     * Payload will have #keySize
     *
     * @return the generated payload
     */
    public String generateRandomKey() {
        return new String(generateRandomBytes(keySize).toString());
    }

    /**
     * Generates random sequence of bytes of size i
     *
     * @return the generated payload
     */
    public byte[] generateRandomBytes(int size) {
        byte[] bytes = new byte[size];
        ranGen.nextBytes(bytes);
        return bytes;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    /**
     * Randomly determines whether a transaction should abort according to
     * #abortRate
     *
     * @return yes if should abort
     */
    public boolean shouldAbort() {
        int ran = ranGen.nextInt(100);
        return (ran < abortRate);
    }

    /**
     * @return the number of operations to execute
     */
    public int getOperationCount() {
        return operationCount;
    }

    /**
     * Computes the size of the next transaction. Transactions contain
     * between 1 and (partitionCount * writesPerPartition) operations
     *
     * @return the (randomly) generated
     */
    public int getTransactionSize() {
        // int ran = ranGen.nextInt(partitionCount * writesPerPartition + 1);
        int ran = partitionCount * writesPerPartition;
        return ran;
    }

    public List<String> getTopics() {
        return topicNames;
    }

    public boolean isTransactional() {
        return transactionMode;
    }

    public int getWritesPerPartition() {
        return writesPerPartition;
    }

    public int getKeySize() {
        return keySize;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public int getAbortRate() {
        return abortRate;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public int getNbProducers() {
        return nbProducers;
    }

    /**
     * Creates a topic with the specified number of partitions. If the topic already exists (even with a different
     * number of partitions or replication factor), this does nothing
     */
    public boolean createTopics() {
        //TODO(ncrooks): make this configurable
        AdminClient client = KafkaAdminClient.create(createClientConfig());

        for (String topicName : topicNames) {
            try {
                NewTopic newTopic = new NewTopic(topicName, partitionCount, replicationFactor);
                CreateTopicsResult createTopicsResult = client.createTopics(Collections.singleton(newTopic));
                // Wait for topic creation to finish by blocking on future
                System.out.format("Wait for topic %s to be created \n", topicName);
                createTopicsResult.values().get(topicName).get();
                System.out.println("Topic successfully created!");
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Received exception during topic creation. Error: " + e.getMessage());
                return false;
            } catch (TopicExistsException e) {
                System.err.println("Warning: Topic already existed. It may have a different number of partitions than expected");
            }
        }
        return true;
    }

    private HashMap<String, Object> createClientConfig() {
        HashMap<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", bootstrapServers);
        config.put("client.id", "kafka-trx");
        return config;
    }

    /**
     * Select a topic uniformly at random
     */
    public String chooseRandomTopic() {
        int ran = ranGen.nextInt(topicNames.size());
        return topicNames.get(ran);
    }
}
