/**
 * Copyright (C), 2018-2018
 * FileName: RocketMQController
 * Author:   WXG
 * Date:     2018/4/19 17:48
 * Description: rocketmq控制器
 */
package com.example.demo.controller;

import com.example.demo.service.RocketMQConsumer;
import com.example.demo.service.RocketMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/test")
public class RocketMQController {
    @Autowired
    private RocketMQProducer producer;
    @Autowired
    private RocketMQConsumer consumer;

    @RequestMapping("/producer")
    public void producer(){
        producer.sendMsgMQ("helloword","147852963");
    }
    
    @RequestMapping("/consumer")
    public void consumer(){
        consumer.consumeMessage();
    }
}
