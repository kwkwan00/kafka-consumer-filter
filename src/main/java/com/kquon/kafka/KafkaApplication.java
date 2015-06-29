package com.kquon.kafka;

import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * Created by kquon on 6/27/15.
 */
public class KafkaApplication {

    private static final Logger logger = Logger.getLogger(KafkaApplication.class);

    private static final String ZOOKEEPER_URL = "localhost:2181";
    private static final String EVENT_TOPIC = "test";
    private static final String GROUP_ID = "test-consumer-group";

    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;

    public KafkaApplication(String zooKeeper, String groupId, String topic) {
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(zooKeeper, groupId));
        this.topic = topic;
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            KafkaApplication kafkaApplication = new KafkaApplication(ZOOKEEPER_URL, GROUP_ID, EVENT_TOPIC);
            try {
                kafkaApplication.run(1);
                Thread.sleep(10000);
            } catch (InterruptedException ie) {

            }
            kafkaApplication.shutdown();
        }
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(Integer a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            // executor.submit(new FilteredEventConsumer(stream /*, threadNumber */));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String zookeeperURL, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeperURL);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("consumer.timeout.ms", "10000");
        return new ConsumerConfig(props);
    }

}