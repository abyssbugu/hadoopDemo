package com.abyss.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * Created by Abyss on 2018/8/13.
 * description:
 */
public class RandomSentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
            "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};

    /**
     * 初始化的一些操作放到这里
     *
     * @param map                  配置信息
     * @param topologyContext      上下文
     * @param spoutOutputCollector 向下游输出数据的收集器
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 处理业务逻辑，在最后向下游输出数据
     */
    @Override
    public void nextTuple() {
        //随机生成句子
        String sentence = sentences[new Random().nextInt(sentences.length)];
        System.out.println("生成的句子是 : " + sentence);

        // 生成消息id，并且把数据存放到messages的map中
        String msgId = UUID.randomUUID().toString();
        System.out.println("生成的句子为 --> " + sentence +", msgid = " + msgId);
        // 发送字符串
        this.collector.emit(new Values(sentence), msgId);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {
        System.out.println(msgId + " --> 处理成功！");
    }

    @Override
    public void fail(Object msgId) {
        System.out.println(msgId + " --> 处理失败！！！");
        // 失败后，要进行重试操作
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //定义向下游输出的名称
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
