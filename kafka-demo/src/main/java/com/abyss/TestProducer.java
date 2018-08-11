package com.abyss;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by Abyss on 2018/8/11.
 * description:
 */
public class TestProducer {
    @Test
    public void testProducer() throws InterruptedException {
        Properties config = new Properties();

        // 设置kafka服务列表，多个用逗号分隔
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        // 设置序列化消息 Key 的类
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置序列化消息 value 的类
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 初始化
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(config);
        for (int i = 0; i < 100 ; i++) {
            ProducerRecord record = new ProducerRecord("my-kafka-topic","data2-" + i);
            // 发送消息
            kafkaProducer.send(record);
            System.out.println("发送消息 --> " + i);

            Thread.sleep(100);
        }

        kafkaProducer.close();

    }
}
