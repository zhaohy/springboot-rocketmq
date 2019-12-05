package com.zhaohy.rocketmq.listener;

import com.zhaohy.rocketmq.event.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * //TODO
 *
 * @author hongyanzhao2
 * @version 1.0.0
 * @since 2019/12/5 17:14
 */
@Component
@Slf4j
public class ConsumerListener {
    @EventListener(condition = "#event.messageExtList[0].topic=='ZHY-TOPIC-TWO' && #event.messageExtList[0].tags=='ZHY-TAG-TWO'")
    public void rocketmqMsgListener(MessageEvent event){
        List<MessageExt> messageExtList = event.getMessageExtList();
        messageExtList.forEach(messageExt -> {
            try {
                log.info("消费消息：{}", new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
            } catch (Exception e) {
                log.error("消费消息异常：", e);
            }
        });
    }
}
