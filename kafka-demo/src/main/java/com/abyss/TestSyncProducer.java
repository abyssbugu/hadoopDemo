package com.abyss;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by Abyss on 2018/8/11.
 * description:
 */
public class TestSyncProducer {
    @Test
    public void testProducer() throws InterruptedException, ExecutionException {
        Properties config = new Properties();

        // 设置kafka服务列表，多个用逗号分隔
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092");
        // 设置序列化消息 Key 的类
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置序列化消息 value 的类
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 初始化
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(config);
        for (int i = 0; i < 10 ; i++) {
            ProducerRecord record = new ProducerRecord("my-kafka-topic","data-" + i);
            // 发送消息
            Future future = kafkaProducer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("消息的callbakc --> " + metadata);
                }
            });
            future.get(); //同步模式进行阻塞
            System.out.println("发送消息 --> " + i);

            Thread.sleep(100);
        }

        kafkaProducer.close();

    }
}
