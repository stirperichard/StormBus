package com.stirperichard.stormbus.kafka;

import com.stirperichard.stormbus.utils.Constants;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleKakfaProducer {

    private final static String PRODUCER_ID = Constants.PRODUCER_GROUPID;

    private String topic;

    private Producer<String, String> producer;

    public SimpleKakfaProducer(String topic) {
        this.topic = topic;
        producer = createProducer();
    }

    private static Producer<String, String> createProducer() {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void produce(String key, String value) {

        try {

            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, key, value);

            RecordMetadata metadata = producer.send(record).get();

            // DEBUG
            System.out.printf("sent record(key=%s value=%s) meta(partition=%d, offset=%d)\n",
                    record.key(), record.value(), metadata.partition(), metadata.offset());


        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    public void close() {
        producer.flush();
        producer.close();
    }
}
