package com.example.streamtunneldemo1;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OdpsConfiguration {


    // 阿里云账号AccessKey ID。
    @Value("${odps.access.id}")
    private String accessId;
    @Value("${odps.access.key}")
    private String accessKey;
    // MaxCompute项目的Endpoint信息，详情请参见配置Endpoint。
    @Value("${odps.url}")
    private String odpsEndpoint;
    // MaxCompute项目的Tunnel Endpoint信息，详情请参见配置Endpoint。
//    private  String tunnelEndpoint = "<tunnel_endpoint>";
    // MaxCompute项目的名称。
    @Value("${odps.project.name}")
    private String project;
    // MaxCompute项目中的表名称。

    // MaxCompute项目中的表的分区信息。
//    private  String partition = "<your_partition_spec>";

    @Value("${odps.tabel.name}")
    public String huyaLiveLogTableName;

    @Value("${odps.batch.size}")
    public int batchSize;

    @Value("${odps.session.life.minute}")
    public int sessionLifeMinute;

    public int getSessionLifeMinute() {
        return sessionLifeMinute;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public String getHuyaLiveLogTableName() {
        return huyaLiveLogTableName;
    }

    public String getAccessId() {
        return accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getOdpsEndpoint() {
        return odpsEndpoint;
    }

//    public String getTunnelEndpoint() {
//        return tunnelEndpoint;
//    }

    public String getProject() {
        return project;
    }

//    public String getTable() {
//        return table;
//    }

}
