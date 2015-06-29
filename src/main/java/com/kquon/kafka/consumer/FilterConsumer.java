package com.kquon.kafka.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import com.kquon.kafka.filter.DataFilter;
import com.kquon.kafka.filter.LogFilter;

/**
 * Created by kquon on 6/27/15.
 */
public class FilterConsumer implements Runnable {

    private final DataFilter dataFilter = new LogFilter();
    private KafkaStream<byte[],byte[]> kafkaStream;

    public FilterConsumer(KafkaStream<byte[], byte[]> kafkaStream) {
        this.kafkaStream = kafkaStream;
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (it.hasNext())
            dataFilter.execute(it.next().message());
    }

}