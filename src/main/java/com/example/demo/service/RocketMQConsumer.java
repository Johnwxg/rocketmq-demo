/**
 * Copyright (C), 2018-2018
 * FileName: RocketMQConsumer
 * Author:   WXG
 * Date:     2018/4/19 16:39
 * Description: rocketmq消费者
 */
package com.example.demo.service;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class RocketMQConsumer {
    private static Map<String, DefaultMQPushConsumer> consumerMap = new HashMap<String, DefaultMQPushConsumer>();

    /**
     * NameServer 地址
     */
    @Value("${rocketmq.namesrvAddr}")
    private String namesrvAddr;

    /**
     * 消费者
     */
    @Value("${rocketmq.topic}")
    private String topic;

    /**
     * 获取消费者
     * @param topic
     * @return
     * @throws Exception
     */
    protected DefaultMQPushConsumer getConsumer(String topic) throws Exception{
        // 如果Map中存在这个key值，则直接返回
        if (consumerMap.containsKey(topic)) {
            return consumerMap.get(topic);
        }
        // 如果不存在，则重新创建Consumer对象
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(topic+"_Consumer_Group");
        consumer.setNamesrvAddr(namesrvAddr);

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        // 从消息队列头开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 订阅主题
        consumer.subscribe(topic, "*");
        return consumer;
    }

    /**
     * 消费者业务逻辑处理
     */
    protected void consumeMessage(){
        try {
            DefaultMQPushConsumer consumer = null;
            try {
                consumer = getConsumer(topic);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
            // 注册消息监听器
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    String json=null;
                    try {
                        json = new String(msgs.get(0).getBody(),"utf-8");
                    } catch (UnsupportedEncodingException e1) {
                        e1.printStackTrace();
                    }
                    return doBusiness(json);
                }
                /**
                 * 业务逻辑
                 *
                 * @param msgContent
                 * @return
                 */
                public ConsumeConcurrentlyStatus doBusiness(String msgContent) {
                    ConsumeConcurrentlyStatus consumerStatus = ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    try {
//                        InputObject inputObject = JsonUtil.json2InputObject(msgContent);
//                        getControlService().execute(inputObject);
                        System.out.print("$$$$$$-消费逻辑处理中");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return consumerStatus;
                }
            });
            // 启动消费端
            consumer.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
