package com.example.streamtunneldemo1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class ThreadPoolConfig {

    @Bean
    public Executor dealMsgThreadPool() {

        // 获取Java虚拟机的可用的处理器数，最佳线程个数，处理器数*2。根据实际情况调整
        int curSystemThreads = Math.max(Runtime.getRuntime().availableProcessors(), 5);
        log.info("------------系统可用线程池个数：" + curSystemThreads);
        // 创建线程池
        ThreadFactory threadFactory = new ThreadFactory() {
            AtomicInteger i = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread("dealMsgThread" + i.getAndIncrement());
            }
        };
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(curSystemThreads, curSystemThreads * 2, 10, TimeUnit.MINUTES, new ArrayBlockingQueue<>(16), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
//        ExecutorService pool = Executors.newFixedThreadPool(curSystemThreads);
        return threadPoolExecutor;

//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//        //配置核心线程数
//        executor.setCorePoolSize(5);
//        //配置最大线程数
//        executor.setMaxPoolSize(5);
//        //配置队列大小
//        executor.setQueueCapacity(50);
//        //配置线程池中的线程的名称前缀
//        executor.setThreadNamePrefix("kafkaThreadPool-");
//        // 设置拒绝策略：当pool已经达到max size的时候，如何处理新任务
//        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
//        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        //执行初始化
//        executor.initialize();
//        return executor;
    }


    @Bean
    public ScheduledThreadPoolExecutor scheduledThreadPool() {

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(5);
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //配置核心线程数
//        executor.setCorePoolSize(5);
        executor.setKeepAliveTime(11, TimeUnit.MINUTES);
        //配置最大线程数
        //配置队列大小
//        executor.;
        //配置线程池中的线程的名称前缀
        executor.setMaximumPoolSize(10);
//        executor.setThreadFactory(r -> new Thread("sessionCommitThread-"));
        // 设置拒绝策略：当pool已经达到max size的时候，如何处理新任务
        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        //执行初始化
//        executor.initialize();
        return executor;
    }
}
