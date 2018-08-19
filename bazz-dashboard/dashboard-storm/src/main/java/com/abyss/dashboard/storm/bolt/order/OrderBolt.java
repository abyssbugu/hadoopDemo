package com.abyss.dashboard.storm.bolt.order;

import com.abyss.dashboard.storm.bean.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;

/**
 * Created by Abyss on 2018/8/19.
 * description:
 */
public class OrderBolt extends BaseBasicBolt {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String json = input.getStringByField("value");
        //System.out.println(json);
        try {
            Order order = MAPPER.readValue(json, Order.class);
            // 业务逻辑处理
            collector.emit(new Values(order));
        } catch (IOException e) {
            e.printStackTrace();
            throw new FailedException();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("order"));
    }
}