package com.example.streamtunneldemo1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;


// https://help.aliyun.com/document_detail/197308.html?spm=a2c4g.11186623.6.673.28c663a0gYrIXM
// https://blog.csdn.net/yuanlong122716/article/details/105160545/     springboot  kafka 的使用


/**
 * mvn  clean  package  -Dmaven.test.skip=true
 * 运行命令
 * <p>
 * <p>
 *      生产环境启动命令
 * nohup java -jar -Dspring.profiles.active=prd   -Dkafka.odps.consumer.threads=4 -Dserver.port=25525     stream-tunnel-demo1-1.0.0.jar   > /dev/null 2>&1  &
 * <p>
 * 向生产机器传输 jar  包
 * <p>nohup java -jar -Dspring.profiles.active=prd    -Dserver.port=2332XXXX ../stream-tunnel-demo1-1.0.0.jar   > /dev/null 2>&1  &
 * wget https://pi-platform-deploy.oss-cn-hangzhou.aliyuncs.com/bigdata/stream-tunnel-demo1-1.0.0.jar
 * <p>
 * <p>
 * jvm 参数 :
 * <p>
 * note :
 * 开发环境部署在机器  172.16.106.138   pi-bigdata-test-001-ali-zjhz
 * 生产环境           pi-kafka-consumer-01
 * <p>
 * 表 t_ods_machine_monitor_info  的字段 dcache_exist
 * 在 开发 和 生产 的 类型是不一样的   int     boolean
 * -XX:PermSize=64M -XX:MaxPermSize=128m
 */



/**
 * Caused by: java.nio.BufferUnderflowException: null
 * Caused by: java.nio.BufferOverflowException: null
 */

@EnableKafka
@SpringBootApplication
public class StreamTunnelDemo1Application {

    public static void main(String[] args) {
        SpringApplication.run(StreamTunnelDemo1Application.class, args);
    }

}
