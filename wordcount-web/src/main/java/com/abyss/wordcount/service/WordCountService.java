package com.abyss.wordcount.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Abyss on 2018/8/14.
 * description:
 */
@Service
public class WordCountService {
    @Autowired
    private RedisTemplate redisTemplate;

    public Map<String, Integer> queryData() {
        Set<String> keys = this.redisTemplate.keys("WordCount:*");
        Map<String, Integer> result = new HashMap<>();
        for (String key : keys) {
            result.put(key.substring(key.indexOf(':') + 1), Integer.parseInt(this.redisTemplate.opsForValue().get(key).toString()));
        }
        return result;
    }
}
