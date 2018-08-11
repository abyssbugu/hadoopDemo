package com.abyss.springkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

/**
 * Created by Abyss on 2018/8/11.
 * description:
 */
public class MyMessageListener implements MessageListener<String, String> {

    public void onMessage(ConsumerRecord<String, String> data) {
        System.out.println("接收到的消息为：" + data);
    }
}
