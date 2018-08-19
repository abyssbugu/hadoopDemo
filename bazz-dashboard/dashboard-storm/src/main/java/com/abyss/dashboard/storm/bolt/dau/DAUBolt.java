package com.abyss.dashboard.storm.bolt.dau;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by Abyss on 2018/8/19.
 * description:
 */
public class DAUBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String value = input.getStringByField("value");
        if(!StringUtils.contains(value,"DAU|")){
            return ;
        }
        value = StringUtils.substringAfter(value,"DAU|");
        System.out.println(value);
        collector.emit(new Values(value));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("value"));
    }

}
