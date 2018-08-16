package com.abyss.bolt.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by Abyss on 2018/8/16.
 * description:整合方式二
 */
public class WordCountStoreMapper implements RedisStoreMapper {

    private RedisDataTypeDescription redisDataTypeDescription;

    public WordCountStoreMapper() {
        // 定义Redis中的数据类型
        this.redisDataTypeDescription =
                new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.STRING);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return this.redisDataTypeDescription;
    }

    @Override
    public String getKeyFromTuple(ITuple iTuple) {
        // 生成redis中的key
        String word = iTuple.getStringByField("word");
        return "WordCount:" + word;
    }

    @Override
    public String getValueFromTuple(ITuple iTuple) {
        // 存储到redis中的值
        Integer count = iTuple.getIntegerByField("count");
        return String.valueOf(count);
    }
}
