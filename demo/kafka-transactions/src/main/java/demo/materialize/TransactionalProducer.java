// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

package demo.materialize;

import kafka.common.KafkaException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.util.Properties;

public class TransactionalProducer {

    /**
     * Demo configuration parameters
     **/
    private final Config configuration;

    private final Producer kafkaProducer;

    /**
     * Configuration properties for Kafka
     */
    private final Properties properties;

    /**
     * Kafka Transaction ID. This is used to identify the same producer instance across process restarts
     */
    private final int transactionId;

    public TransactionalProducer(Config pConfig, int offset) {
        this.configuration = pConfig;
        this.transactionId = pConfig.getTransactionId() + offset;
        this.properties = createProperties(pConfig.isTransactional());
        this.kafkaProducer = new KafkaProducer<byte[], byte[]>(properties);
    }

    public int startGeneratingData() {

        System.out.println("Start Generating Data for Producer " + transactionId);
        int opToExecute = configuration.getOperationCount();
        int opExecuted = 0;
        boolean useTransaction = configuration.isTransactional();

        if (useTransaction) kafkaProducer.initTransactions();

        while (opToExecute > 0) {

            int trxSize = configuration.getTransactionSize();
            boolean shouldAbort = configuration.shouldAbort();
            boolean isAborted = false;

            if (useTransaction) {
                kafkaProducer.beginTransaction();
                isAborted = false;
            }
            for (int i = 0; i < trxSize; i++) {
                try {
                    kafkaProducer.send(new ProducerRecord(configuration.chooseRandomTopic(), configuration.generateRandomKey(), configuration.generateRandomPayload()));
                } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                    // Cannot recover from these exceptions. Close the producer and exit
                    System.err.println("Error: " + e.toString() + "\n Exiting ...");
                    kafkaProducer.close();
                    System.exit(-1);
                } catch (KafkaException e) {
                    System.err.println("Error: " + e.toString() + "\n Aborting ");
                    kafkaProducer.abortTransaction();
                    isAborted = true;
                }
            }

            // If executing in transactional mode, and haven't already aborted the trx due to error
            if (useTransaction && !isAborted) {
                if (shouldAbort) {
                    System.out.println("Aborting transaction");
                    kafkaProducer.abortTransaction();
                } else {
                    opExecuted += trxSize;
                    kafkaProducer.commitTransaction();
                }
            }

            opToExecute -= trxSize;
        }

        System.out.format("Executed a total of %s operations. This corresponds to the total number of operations that were " +
            "successfully committed. \n ", opExecuted);
        kafkaProducer.flush();
        kafkaProducer.close();

        return opExecuted;
    }

    /**
     * Creates properties for Kafka Producer
     *
     * @return the property set
     */
    private Properties createProperties(boolean isTransactional) {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", configuration.getBootstrapServers());
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 5);
        // TODO(natacha): add support for avro
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        if (isTransactional) {
            props.put("transactional.id", String.valueOf(transactionId));
            props.put("enable.idempotence", true);
        }
        return props;
    }

}
