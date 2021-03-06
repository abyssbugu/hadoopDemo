package com.abyss.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Abyss on 2018/8/13.
 * description:分割句子
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
//        String sentence = tuple.getStringByField("sentence");
        String sentence = tuple.getStringByField("value");
        System.out.println("接收到消息为 --> " + sentence);
        // 进行分割处理
        String[] words = sentence.split(" ");
        for (String word : words) {
            outputCollector.emit(tuple, new Values(word));
        }

        // 成功
        this.outputCollector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));

    }
}
