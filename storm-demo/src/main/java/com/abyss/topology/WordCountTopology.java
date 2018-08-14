package com.abyss.topology;

import com.abyss.bolt.PrintBolt;
import com.abyss.bolt.RedisBolt;
import com.abyss.bolt.SplitSentenceBolt;
import com.abyss.bolt.WordCountBolt;
import com.abyss.spout.RandomSentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
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
        builder.setSpout("RandomSentenceSpout", new RandomSentenceSpout(),2).setNumTasks(2);
        builder.setBolt("SplitSentenceBolt", new SplitSentenceBolt(),4).localOrShuffleGrouping("RandomSentenceSpout")
        .setNumTasks(4);
        builder.setBolt("WordCountBolt", new WordCountBolt(),2)
                .partialKeyGrouping("SplitSentenceBolt",new Fields("word"));
//        builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("WordCountBolt");
        builder.setBolt("RedisBolt", new RedisBolt()).localOrShuffleGrouping("WordCountBolt");
        //第三步，构建Topology对象
        StormTopology topology = builder.createTopology();
        Config config = new Config();

        //设置工作进程数
        config.setNumWorkers(2);

        if (args == null || args.length == 0) {
            // 本地模式

            //第四步，提交拓扑到集群，这里先提交到本地的模拟环境中进行测试
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("WordCountTopology", config, topology);
        } else {
            // 集群模式

            config.setNumWorkers(2); // 设置工作进程数
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
