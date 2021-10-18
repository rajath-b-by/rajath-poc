package com.mykafka.kafkacustompartitioner;

import com.mykafka.partitioner.TenantPartitioner;
import com.mykafka.producer.KafkaProducerConfig;
import com.mykafka.producer.MyProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = {MyProducer.class, KafkaProducerConfig.class, TenantPartitioner.class})
class KafkaCustomPartitionerApplicationTests {

    /*@Autowired
    private KafkaConsumer consumer;*/

    @Autowired
    private MyProducer producer;


    private String topic = "tenant-topic";

    @Test
    public void testMessageSend() throws Exception {
        //producer.sendMessage("sending test message",topic);
        producer.sendPartitionSpecificMessage(topic, "1001","testmessage-1");
        producer.sendPartitionSpecificMessage(topic, "1002","testmessage-2");
        producer.sendPartitionSpecificMessage(topic, "1003","testmessage-3");
        producer.sendPartitionSpecificMessage(topic, "1001","testmessage-4");
        producer.sendPartitionSpecificMessage(topic, "1002","testmessage-5");
        producer.sendPartitionSpecificMessage(topic, "1003","testmessage-6");

    }

}
