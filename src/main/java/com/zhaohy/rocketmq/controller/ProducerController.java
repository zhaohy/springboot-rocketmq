package com.zhaohy.rocketmq.controller;

import com.alibaba.fastjson.JSON;
import com.zhaohy.rocketmq.beans.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

/**
 * //TODO
 *
 * @author hongyanzhao2
 * @version 1.0.0
 * @since 2019/12/5 16:40
 */
@RestController
@RequestMapping("/send")
@Slf4j
public class ProducerController {
    @Autowired
    private DefaultMQProducer defaultMQProducer;

    @Autowired
    private TransactionMQProducer transactionMQProducer;

    /**
     * 发送普通消息
     * @return 发送结果
     */
    @GetMapping("defaultMessage")
    public String defaultMessage(){
        try {
            int sendTotal = 100;
            Message message = new Message();
            message.setTopic("ZHY-TOPIC-TWO");
            message.setTags("ZHY-TAG-TWO");
            for (int i = 0; i < sendTotal; i++) {
                User user = new User(getUUID(), String.format("当前是第【%d】条消息", i));
                String json = JSON.toJSONString(user);
                message.setBody(json.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.setKeys(user.getId());
                SendResult sendResult = defaultMQProducer.send(message);
                log.info("发送消息msgId：{}， 发送状态：{}", sendResult.getMsgId(), sendResult.getSendStatus());
            }
            return "finish";
        } catch (Exception e) {
            log.error("普通消息发送异常：", e);
            return "failed";
        }
    }

    /**
     * 发送事务消息
     * @return
     */
    @GetMapping("/transactionMessage")
    public String transactionMessage(@RequestParam("param") String param){
        SendResult sendResult = null;
        try {
            Message message = new Message();
            message.setTopic("ZHY-TOPIC-TWO");
            message.setTags("ZHY-TAG-TWO");
            message.setBody(param.getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.setKeys(getUUID());
            transactionMQProducer.setTransactionCheckListener(messageExt -> {
                log.info("TransactionCheckListener");
                return LocalTransactionState.COMMIT_MESSAGE;
            });
            transactionMQProducer.setTransactionListener(new TransactionListener() {
                @Override
                public LocalTransactionState executeLocalTransaction(Message message, Object arg) {
                    log.info("处理本地事务，topic【{}】，tags【{}】，keys【{}】", message.getTopic(), message.getTags(), message.getKeys());
                    String value = "";
                    if (arg instanceof String) {
                        value = (String) arg;
                    }
                    if (StringUtils.isEmpty(value)) {
                        log.warn("发送消息不能为空...");
                        throw new RuntimeException("发送消息不能为空...");
                    } else if ("a".equals(value)) {
                        return LocalTransactionState.ROLLBACK_MESSAGE;
                    } else if ("b".equals(value)) {
                        return LocalTransactionState.COMMIT_MESSAGE;
                    }
                    //处理本地事务
                    return LocalTransactionState.COMMIT_MESSAGE;
                }

                @Override
                public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                    //检查本地事务
                    log.info("检查本地事务，topic【{}】，tags【{}】，keys【{}】", messageExt.getTopic(), messageExt.getTags(), messageExt.getKeys());
                    return LocalTransactionState.COMMIT_MESSAGE;
                }
            });
            sendResult = transactionMQProducer.sendMessageInTransaction(message, param);
            log.info("发送事务消息响应结果：{}", sendResult);
            return sendResult.toString();
        } catch (Exception e) {
            log.error("发送事务消息：", e);
            return "failed";
        }
    }

    @GetMapping("/orderMessage")
    public String orderMessage(){
        try {
            int sendTotal = 1;
            Message message = new Message();
            message.setTopic("ZHY-TOPIC-TWO");
            message.setTags("ZHY-TAG-TWO");
            for (int i = 0; i < sendTotal; i++) {
                User user = new User(getUUID(), String.format("当前是第【%d】条消息", i));
                String json = JSON.toJSONString(user);
                message.setBody(json.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.setKeys(user.getId());
                SendResult sendResult = defaultMQProducer.send(message, (messageQueueList, message1, obj) -> {
                    int index = ((Integer) obj) % messageQueueList.size();
                    return messageQueueList.get(index);
                }, i);
                log.info("发送消息msgId：{}， 发送状态：{}", sendResult.getMsgId(), sendResult.getSendStatus());
            }
            return "finish";
        } catch (Exception e) {
            log.error("普通消息发送异常：", e);
            return "failed";
        }
    }

    private String getUUID(){
        return UUID.randomUUID().toString().replace("-", "");
    }
}
