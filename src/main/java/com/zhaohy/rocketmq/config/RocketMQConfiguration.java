package com.zhaohy.rocketmq.config;

import com.zhaohy.rocketmq.event.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * //TODO
 *
 * @author hongyanzhao2
 * @version 1.0.0
 * @since 2019/12/5 15:40
 */
@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@Slf4j
public class RocketMQConfiguration {
    @Autowired
    private RocketMQProperties rocketMQProperties;

    /**
     * 事件监听
     */
    @Autowired
    private ApplicationEventPublisher publisher;
    /**
     * 是否第一次订阅
     */
    private static boolean isFirstSub = Boolean.TRUE;
    private static long startTime = System.currentTimeMillis();

    @Resource(name = "consumerThreadPool")
    private ExecutorService consumerThreadPool;

    /**
     * 窗口初始化的时候打印参数信息
     */
    @PostConstruct
    public void init(){
        log.info(rocketMQProperties.toString());
    }

    /**
     * 创建普通生产者实例
     * @return
     */
    @Bean
    public DefaultMQProducer defaultMQProducer(){
        DefaultMQProducer producer = null;
        try {
            producer = new DefaultMQProducer(rocketMQProperties.getProducerGroupName());
            producer.setNamesrvAddr(rocketMQProperties.getNamesrvAddr());
            producer.setInstanceName(rocketMQProperties.getProducerInstanceName());
            producer.setVipChannelEnabled(Boolean.FALSE);
            producer.setRetryTimesWhenSendAsyncFailed(10);
            producer.start();
            log.info("rocketmq default producer server is starting ......");
        } catch (MQClientException e) {
            log.error("创建普通生产者实例失败：", e);
        }
        return producer;
    }

    /**
     * 创建事务型生产者实例
     * @return
     */
    @Bean
    public TransactionMQProducer transactionMQProducer(){
        TransactionMQProducer producer = null;
        try {
            producer = new TransactionMQProducer(rocketMQProperties.getTransactionProducerGroupName());
            producer.setNamesrvAddr(rocketMQProperties.getNamesrvAddr());
            producer.setInstanceName(rocketMQProperties.getProducerTranInstanceName());
            producer.setRetryTimesWhenSendAsyncFailed(10);
            producer.start();
            log.info("rocketmq transaction producer server is starting ......");
        } catch (MQClientException e) {
            log.error("创建普通生产者实例失败：", e);
        }
        return producer;
    }

    @Bean
    public DefaultMQPushConsumer pushConsumer() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(rocketMQProperties.getConsumerGroupName());
        consumer.setNamesrvAddr(rocketMQProperties.getNamesrvAddr());
        consumer.setInstanceName(rocketMQProperties.getConsumerInstanceName());
        String messageModel = "非广播模式";
        if(rocketMQProperties.isConsumerBroadcasting()){
            messageModel = "广播模式";
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
        log.info(messageModel);
        //设置批量消费
        consumer.setConsumeMessageBatchMaxSize(rocketMQProperties.getConsumerBatchMaxSize() == 0 ? 1 : rocketMQProperties.getConsumerBatchMaxSize());
        //获取topic及tags
        List<String> subscribeList = rocketMQProperties.getSubscribe();
        for (String s : subscribeList) {
            String[] arr = s.split(":");
            consumer.subscribe(arr[0], arr[1]);
        }
        if(rocketMQProperties.isEnableOrderConsumer()){
            consumer.registerMessageListener((MessageListenerOrderly)(msgs, context) ->{
                try {
                    context.setAutoCommit(Boolean.TRUE);
                    msgs = filterMessage(msgs);
                    if(msgs.size() == 0){
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                    publisher.publishEvent(new MessageEvent(msgs, consumer));
                    log.info("顺序消费，message：{}", msgs);
                } catch (Exception e) {
                    log.error("顺序消费异常：", e);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                return ConsumeOrderlyStatus.SUCCESS;
            });
        }else {
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) ->{
                try {
                    msgs = filterMessage(msgs);
                    if(msgs.size() == 0){
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    publisher.publishEvent(new MessageEvent(msgs, consumer));
                    log.info("并发消费，message：{}", msgs);
                } catch (Exception e) {
                    log.error("并发消费异常：", e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
        }
        consumerThreadPool.execute(()->{
            try {
                Thread.sleep(5000);
                consumer.start();
                log.info("rocketmq server consumer is starting......");
            } catch (Exception e) {
                log.error("消费者创建异常", e);
            }
        });
        return consumer;
    }

    private List<MessageExt> filterMessage(List<MessageExt> messageExtList){
        if(isFirstSub && !rocketMQProperties.isEnableHistoryConsumer()){
            messageExtList = messageExtList.stream().filter(messageExt -> startTime - messageExt.getBornTimestamp() < 0).collect(Collectors.toList());
        }
        if(isFirstSub && messageExtList.size() >0 ){
            isFirstSub = Boolean.FALSE;
        }
        return messageExtList;
    }

}
