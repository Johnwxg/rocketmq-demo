/**
 * Copyright (C), 2018-2018
 * FileName: RocketMQConfig
 * Author:   WXG
 * Date:     2018/4/19 15:52
 * Description: rocketmq配置
 */
package com.example.demo.service;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 〈rocketmq配置〉
 *
 * @Author wxg
 * @Date 2018/4/19
 * @since 1.0.0
 */
@Component
public class RocketMQProducer {
    private static Map<String, DefaultMQProducer> producerMap = new HashMap<String, DefaultMQProducer>();

    /**
     * 生产者
     */
    @Value("${rocketmq.topic}")
    private String topic;

    /**
     * NameServer 地址
     */
    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;

    /**
     * 获取生产者
     * @param
     * @return 生产者
     */
    public DefaultMQProducer getProducer(){//(String topic){
        if (producerMap.containsKey(topic)) {
            return producerMap.get(topic);
        }
        DefaultMQProducer producer = new DefaultMQProducer(topic+"_PROD_GROUP");
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName(topic);
        try {
            producer.start();
        }catch (Exception e){
            e.printStackTrace();
        }
        producerMap.put(topic, producer);
        return producer;
    }

    /**
     * 将内容放入消息队列
     * @param
     * @param key
     * @param value
     * @return
     */
    public boolean sendMsgMQ(String key, String value){//(String topic, String key, String value){
        if (topic==null || value==null) {
            return false;
        }
//        DefaultMQProducer producer = getProducer(topic);
        DefaultMQProducer producer = getProducer();
        Message msg;
        SendResult result = null;
        try {
            msg = new Message(topic,null,key,value.getBytes("utf-8"));
            result = producer.send(msg);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.print(result.getSendStatus().toString());
        return SendStatus.SEND_OK.equals(result.getSendStatus());
    }
}
