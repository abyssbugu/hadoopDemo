package com.abyss.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Abyss on 2018/8/13.
 * description:
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector outputCollector;

    private Map<String, Integer> map = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = map.get(word);
        if (count == null) {
            count = 0;
        }
        count++;
        map.put(word, count);
        outputCollector.emit(tuple,new Values(word, count));

        //这里模拟有时成功有时失败
        if(System.currentTimeMillis() % 2 == 0){
            // 偶数，成功
            outputCollector.ack(tuple);
        }else{
            // 奇数，失败
            outputCollector.fail(tuple);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));

    }
}
