package com.abyss;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by Abyss on 2018/8/11.
 * description:创建topic
 */
public class TestKafkaTopic {
    @Test
    public void testCreateTopic() {
        ZkUtils zkUtils = null;
        try {
            //参数：zookeeper的地址，session超时时间，连接超时时间，是否启用zookeeper安全机制
            zkUtils = ZkUtils.apply("node1:2181", 30000, 3000, JaasUtils.isZkSecurityEnabled());

            String topicName = "my-kafka-topic2";
            if (!AdminUtils.topicExists(zkUtils, topicName)) {
                //参数：zkUtils，topic名称，partition数量，副本数量，参数，机架感知模式
                AdminUtils.createTopic(zkUtils, topicName, 3, 1, new Properties(), AdminUtils.createTopic$default$6());
                System.out.println(topicName + " 创建成功!");
            } else {
                System.out.println(topicName + " 已存在!");
            }
        } finally {
            if (null != zkUtils) {
                zkUtils.close();
            }
        }

    }

    @Test
    public void testDeleteTopic() {
        ZkUtils zkUtils = null;
        try {
            //参数：zookeeper的地址，session超时时间，连接超时时间，是否启用zookeeper安全机制
            zkUtils = ZkUtils.apply("node1:2181", 30000, 3000, JaasUtils.isZkSecurityEnabled());
            String topicName = "my-kafka-topic-test1";
            if (AdminUtils.topicExists(zkUtils, topicName)) {
                //参数：zkUtils，topic名称，partition数量，副本数量，参数，机架感知模式
                AdminUtils.deleteTopic(zkUtils, topicName);
                System.out.println(topicName + " 删除成功!");
            } else {
                System.out.println(topicName + " 不已存在!");
            }
        } finally {
            if (null != zkUtils) {
                zkUtils.close();
            }
        }
    }
}
