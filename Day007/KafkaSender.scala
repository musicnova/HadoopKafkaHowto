package ru.croc.smartdata.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 *
 */
public class KafkaSender {

    private Producer<Integer, byte[]> producer;

    KafkaSender() {
        createProducer(null);
    }

    KafkaSender(Properties addProps) {
        createProducer(addProps);
    }

    public void send(String topicName, byte[] msg) {
        ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topicName, msg);
        producer.send(record);
    }

    public void close() {
        producer.close();
    }

    private void createProducer(Properties addProps) {
        Properties props = new Properties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        if(addProps != null) {
            props.putAll(addProps);
        }
        producer = new KafkaProducer<>(props);
    }
}
