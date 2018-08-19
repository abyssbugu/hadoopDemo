package com.abyss.dashboard.generate.service.impl;

import com.abyss.dashboard.generate.bean.Order;
import com.abyss.dashboard.generate.service.IGenerate;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.text.DecimalFormat;
import java.util.Date;
import java.util.UUID;

/**
 * Created by Abyss on 2018/8/16.
 * description:
 */
@Component
public class OrderGenerateImpl implements IGenerate {

    @Autowired
    private RedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    public void start() {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    try {
                        generateOrder();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void generateOrder() throws Exception {
        Long sleep = RandomUtils.nextLong(200, 1000 * 5);
        Thread.sleep(sleep);

        Order order = new Order();
        Long maxUserId = Long.valueOf(this.redisTemplate.opsForValue().get("dashboard-generate-user-max-id").toString());
        Long userId = RandomUtils.nextLong(1, maxUserId);
        order.setUserId(userId);
        order.setOrderId(StringUtils.replace(UUID.randomUUID().toString(),"-",""));
        order.setPayment(Double.valueOf(new DecimalFormat("#.00").format(RandomUtils.nextDouble(100,20000))));
        order.setPaymentType(1); //默认都是在线支付
        order.setPostFee("0");
        order.setStatus(RandomUtils.nextInt(1,3));
        order.setCreateTime(new Date());
        order.setUpdateTime(order.getCreateTime());
        if(order.getStatus().intValue() == 2){
            order.setPaymentTime(order.getCreateTime());
        }

        this.kafkaTemplate.send("topic-dashboard-generate-order", order);

    }

}
