package com.abyss.topology;

import com.abyss.bolt.SplitSentenceBolt;
import com.abyss.bolt.WordCountBolt;
import com.abyss.bolt.jdbc.JdbcBoltBuilder;
import com.abyss.bolt.redis.RedisBolt;
import com.abyss.bolt.redis.WordCountStoreMapper;
import com.abyss.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by Abyss on 2018/8/13.
 * description:
 */
public class WordCountTopology {
    public static void main(String[] args) {
        //第一步，定义TopologyBuilder对象，用于构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        //第二步，设置spout和bolt
//        builder.setSpout("RandomSentenceSpout", new RandomSentenceSpout());
//        builder.setBolt("SplitSentenceBolt", new SplitSentenceBolt()).localOrShuffleGrouping("RandomSentenceSpout");

        KafkaSpoutConfig.Builder<String, String> kafkaSpoutBuilder = KafkaSpoutConfig.builder("node1:9092", "kafka-storm-topic");
        kafkaSpoutBuilder.setGroupId("kafka-storm-topic-consumer-groupid"); //设置消费者组id

        // 这里设置Spout的并行度为3，原因是创建topic时，指定的partition为3
        builder.setSpout("kafka_spout", new KafkaSpout<>(kafkaSpoutBuilder.build()),3);
        builder.setBolt("SplitSentenceBolt", new SplitSentenceBolt()).localOrShuffleGrouping("kafka_spout");


        builder.setBolt("WordCountBolt", new WordCountBolt()).partialKeyGrouping("SplitSentenceBolt", new Fields("word"));

        //打印数据
        //builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("WordCountBolt");

        //数据写入到redis
//        方式一
//        builder.setBolt("RedisBolt", new RedisBolt()).localOrShuffleGrouping("WordCountBolt");
//        方式二
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("node1").setPort(6379).build();
        builder.setBolt("RedistBolt", new RedisStoreBolt(poolConfig, new WordCountStoreMapper()))
                .localOrShuffleGrouping("WordCountBolt");



        // 数据写入到mysql数据库中
//        builder.setBolt("jdbcBolt",JdbcBoltBuilder.build()).localOrShuffleGrouping("WordCountBolt");


        //第三步，构建Topology对象
        StormTopology topology = builder.createTopology();
        Config config = new Config();
        if (args == null || args.length == 0) {
            // 本地模式

            //第四步，提交拓扑到集群，这里先提交到本地的模拟环境中进行测试
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("WordCountTopology", config, topology);
        } else {
            config.setNumWorkers(2); // 设置工作进程数
            config.setMessageTimeoutSecs(100);
            config.setNumAckers(2);
            // 集群模式
            try {
                //提交到集群，并且将参数作为拓扑的名称
                StormSubmitter.submitTopology(args[0], config, topology);
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }

    }
}
