package com.zhaohy.rocketmq.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * //TODO
 *
 * @author hongyanzhao2
 * @version 1.0.0
 * @since 2019/12/5 16:15
 */
@Configuration
public class ThreadPoolConfig {
    @Bean(name = "consumerThreadPool")
    public ExecutorService buildConsumerThreadPool(){
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1), r -> {
                    Thread thread = new Thread(r);
                    thread.setName("rocketmq-consumer-thread-%d");
                    return thread;
                });
    }
}
