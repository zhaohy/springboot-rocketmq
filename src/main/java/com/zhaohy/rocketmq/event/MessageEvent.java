package com.zhaohy.rocketmq.event;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.ApplicationEvent;

import java.util.List;

/**
 * //TODO
 *
 * @author hongyanzhao2
 * @version 1.0.0
 * @since 2019/12/5 16:08
 */
@Setter
@Getter
public class MessageEvent extends ApplicationEvent {
    private DefaultMQPushConsumer consumer;
    private List<MessageExt> messageExtList;

    public MessageEvent(List<MessageExt> messageExtList, DefaultMQPushConsumer consumer) {
        super(messageExtList);
        this.consumer = consumer;
        this.messageExtList = messageExtList;
    }
}
