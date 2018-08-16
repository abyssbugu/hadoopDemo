package com.abyss.bolt.redis;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;

/**
 * Created by Abyss on 2018/8/14.
 * description:Redis整合方式一
 */
public class RedisBolt extends BaseRichBolt {
    private JedisPool jedisPool;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.jedisPool = new JedisPool(new JedisPoolConfig(), "node1", 6379);
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Integer count = tuple.getIntegerByField("count");

        String key = "WordCount:" + word;
        // 保存到redis中的key
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            jedis.set(key, String.valueOf(count));
        } finally {
            if (null != jedis) {
                jedis.close();
            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
