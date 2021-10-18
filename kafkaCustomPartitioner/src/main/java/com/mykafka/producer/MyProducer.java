package com.mykafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * @author rajath.b
 * A kafka producer class. It has methods to send messages to the broker.
 */
@Component
public class MyProducer {
    private KafkaTemplate<String, String> kafkaTemplate;
    private Producer<String, String> kafkaProducer;

    private static final Logger LOGGER = LoggerFactory.getLogger(MyProducer.class);

    @Autowired
    MyProducer(KafkaTemplate<String, String> kafkaTemplate, Producer<String, String> producer) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProducer = producer;
    }

    public void sendMessage(String message, String topicName) {
        LOGGER.info("sending message='{}' to topic='{}'", message, topicName);
        kafkaTemplate.send(topicName, message);
    }

    public void sendPartitionSpecificMessage( String topicName, String key, String message) {
        LOGGER.info("sending message ='{}' to topic='{}' in partition", message, topicName);
        kafkaProducer.send(new ProducerRecord<>(topicName, key, message));
    }
}
