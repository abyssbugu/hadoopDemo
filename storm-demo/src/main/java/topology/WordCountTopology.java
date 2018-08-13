package topology;

import bolt.PrintBolt;
import bolt.SplitSentenceBolt;
import bolt.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import spout.RandomSentenceSpout;

/**
 * Created by Abyss on 2018/8/13.
 * description:
 */
public class WordCountTopology {
    public static void main(String[] args) {

        //第一步，定义TopologyBuilder对象，用于构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        //第二步，设置spout和bolt
        builder.setSpout("RandomSentenceSpout", new RandomSentenceSpout());
        builder.setBolt("SplitSentenceBolt", new SplitSentenceBolt()).shuffleGrouping("RandomSentenceSpout");
        builder.setBolt("WordCountBolt", new WordCountBolt()).shuffleGrouping("SplitSentenceBolt");
        builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("WordCountBolt");

        //第三步，构建Topology对象
        StormTopology topology = builder.createTopology();

        //第四步，提交拓扑到集群，这里先提交到本地的模拟环境中进行测试
        new LocalCluster().submitTopology("WordCountTopology", new Config(), topology);

    }
}
