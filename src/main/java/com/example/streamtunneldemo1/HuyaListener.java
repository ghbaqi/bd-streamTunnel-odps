package com.example.streamtunneldemo1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


//{"EventType":"ecdnl-access-log","QoSType":"ecdnv4","Timestamp":"2021-04-02 14:30:33 +0800","_lsvc_time":1617345034,"app_id":"huya-slice","app_name":"huyalive","body_bytes_sent":49838,"bytes_sent":49838,"cdn_user_agent":"cdn_agent_ppio","hit_info":"TCP_HIT -","host":"psource.huya.com","hostname":"1424e603","http_referer":"","http_user_agent":"android, 20000313_APP(adr\u00268.12.2\u0026huawei\u002629)","huya_uid":"H_3402589098_Y","ip":"183.93.250.142","is_complete":true,"isp":"联通","kj_id":"1424e603","province":"湖北省","region":"华东","remote_addr":"58.240.167.138","request":"GET /huyalive/1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16.pstream?from=ppio\u0026uid=H_3402589098_Y\u0026wsSecret=a061328868f61ed4b7d1f549e3fdc4d7\u0026wsTime=6066b9fa\u0026line=7\u0026isUsePsdk=0\u0026isTestEnv=0\u0026hotgather=0\u0026ctype=huya_adr\u0026txyp=o:cs6;\u0026fs=bgct\u0026\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\u0026sphdDC=huya\u0026sphd=264_*-265_*\u0026baseIndex=6173318563006448\u0026u=5999874663\u0026t=2 HTTP/1.1","request_at":1617345020025,"request_id":"3ddfb487-e1a9-4193-9bf6-4bf83efe3add","request_time":13.022,"scheme":"HTTP","scheme_hdr":"http","scheme_sup":"http","status":200,"stream_name":"1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16","unix_timestamp":1617345033022,"upstream_addr":"-","upstream_response_time":12.997,"url":"http://183.93.250.142:35711/huyalive/1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16.pstream?from=ppio\u0026uid=H_3402589098_Y\u0026wsSecret=a061328868f61ed4b7d1f549e3fdc4d7\u0026wsTime=6066b9fa\u0026line=7\u0026isUsePsdk=0\u0026isTestEnv=0\u0026hotgather=0\u0026ctype=huya_adr\u0026txyp=o:cs6;\u0026fs=bgct\u0026\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\u0026sphdDC=huya\u0026sphd=264_*-265_*\u0026baseIndex=6173318563006448\u0026u=5999874663\u0026t=2","uuid":"994dc817-6928-4c05-aa22-7eef2ddb8e26","version":"v0.3.13","vhost":"hy.ppio-cloud.com"}
//{"EventType":"ecdnl-access-log","QoSType":"ecdnv4","Timestamp":"2021-04-02 14:30:33 +0800","_lsvc_time":1617345034,"app_id":"huya-slice","app_name":"huyalive","body_bytes_sent":221785,"bytes_sent":221785,"cdn_user_agent":"cdn_agent_ppio","hit_info":"TCP_HIT -","host":"psource.huya.com","hostname":"1424e603","http_referer":"","http_user_agent":"HYSDK(iOS, 30000002)_APP(ios\u00268.12.0\u0026store\u002614.4.2)","huya_uid":"H_4675609904_Y","ip":"183.93.250.142","is_complete":true,"isp":"联通","kj_id":"1424e603","province":"湖北省","region":"华东","remote_addr":"221.12.121.168","request":"GET /huyalive/1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16.pstream?from=ppio\u0026uid=H_4675609904_Y\u0026wsSecret=8c75b0a8a3d2d2e37d1b85f3e91324a1\u0026wsTime=6066b9f3\u0026line=3\u0026isUsePsdk=0\u0026isTestEnv=0\u0026hotgather=0\u0026ctype=huya_ios\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\u0026sphdDC=huya\u0026sphd=264_*-265_*\u0026fs=gctex\u0026baseIndex=6173270974838593\u0026u=827099247499\u0026t=3 HTTP/1.1","request_at":1617345012254,"request_id":"ddf9abcd-98fd-4450-b40c-86ec4b305ee6","request_time":21.276,"scheme":"HTTP","scheme_hdr":"http","scheme_sup":"http","status":200,"stream_name":"1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16","unix_timestamp":1617345033276,"upstream_addr":"-","upstream_response_time":21.022,"url":"http://183.93.250.142:35711/huyalive/1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16.pstream?from=ppio\u0026uid=H_4675609904_Y\u0026wsSecret=8c75b0a8a3d2d2e37d1b85f3e91324a1\u0026wsTime=6066b9f3\u0026line=3\u0026isUsePsdk=0\u0026isTestEnv=0\u0026hotgather=0\u0026ctype=huya_ios\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\u0026sphdDC=huya\u0026sphd=264_*-265_*\u0026fs=gctex\u0026baseIndex=6173270974838593\u0026u=827099247499\u0026t=3","uuid":"7fa6e818-9cbd-426f-b18b-160d5bb77713","version":"v0.3.13","vhost":"hy.ppio-cloud.com"}
@Component
@Slf4j
public class HuyaListener {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private ExecutorService dealMsgThreadPool;

    @Autowired
    private OdpsConfiguration odpsConfiguration;

    //    String[] dts = {"20210301","20210302","20210303","20210304"};
//    String[] dts = {"20210301"};

//    public static void main(String[] args) {
//        long etLong = 1617345033276L;
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
//        String dt = simpleDateFormat.format(new Date(etLong));
//        System.out.println(dt);
//    }


    @KafkaListener(topics = {"ecdnLiveAccess"}, concurrency = "${kafka.odps.consumer.threads}")
    public void listen(List<ConsumerRecord<String, String>> crs) {
        for (ConsumerRecord<String, String> cr : crs) {
//            dealMsgThreadPool.submit(new DealMsgTask(cr));
//            if (5 / 1 > 0) {
//                continue;
//            }
            JSONObject jsonObject;
            Long etLong;
            try {
                jsonObject = JSON.parseObject(cr.value());
//           jsonObject = JSON.parseObject(msg);
                etLong = jsonObject.getLong("unix_timestamp");

                // 丢弃脏数据
                if (etLong == null) {
                    log.error("没有 unix_timestamp 字段 , {}", jsonObject);
                    return;
                }
//            jsonObject.put("kafka_time", cr.timestamp() / 1000);
                jsonObject.put("kafka_time", cr.timestamp());
            } catch (Exception e) {
                log.error("错误的消息格式 msg = {}", cr.value());
//            log.error("错误的消息格式 msg = {}", msg);
                return;
            }

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String dt = simpleDateFormat.format(new Date(etLong));

//        Random random = new Random();
//        dt = dts[random.nextInt(dts.length)];

            try {

                TableTunnel.StreamUploadSession session = sessionManager.getSession(odpsConfiguration.getHuyaLiveLogTableName() + "&" + dt);
                Record record = session.newRecord();
                TableSchema schema = sessionManager.getTableSchema();
                for (int i = 0; i < schema.getColumns().size(); i++) {
                    Column column = schema.getColumn(i);

                    switch (column.getType()) {
                        case BIGINT:
                            record.set(i, jsonObject.getLongValue(column.getName()));
                            break;
                        case BOOLEAN:
                            record.set(i, jsonObject.getBooleanValue(column.getName()));
                            //                    record.setBoolean(i, false);
                            break;
                        case DATETIME:
                            record.set(i, jsonObject.getDate(column.getName()));
                            //                    record.setDatetime(i, new Date());
                            break;
                        case DOUBLE:
                            record.set(i, jsonObject.getDoubleValue(column.getName()));
                            //                    record.setDouble(i, 2000.25);
                            break;
                        case STRING:
                            record.setString(i, jsonObject.getString(column.getName()));
                            //                    record.setString(i, "aaaab");
                            break;

                        case INT:
                            record.set(i, jsonObject.getIntValue(column.getName()));
                            break;

                        default:
                            throw new RuntimeException("Unknown column type: " + column.getType());
                    }
                }
                TableTunnel.StreamRecordPack recordPack = sessionManager.getRecordPack(odpsConfiguration.getHuyaLiveLogTableName(), dt);
                recordPack.append(record);
            } catch (TunnelException | IOException e) {
                log.error("写入错误 data = {} , Exception = {} ", cr.value(), e);
            }

        }
    }

    private class DealMsgTask implements Runnable {

        private ConsumerRecord<String, String> cr;

        public DealMsgTask(ConsumerRecord<String, String> cr) {
            this.cr = cr;
        }

        @Override
        public void run() {
            JSONObject jsonObject;
            Long etLong;
            try {
                jsonObject = JSON.parseObject(cr.value());
//           jsonObject = JSON.parseObject(msg);
                etLong = jsonObject.getLong("unix_timestamp");

                // 丢弃脏数据
                if (etLong == null) {
                    log.error("没有 unix_timestamp 字段 , {}", jsonObject);
                    return;
                }
//            jsonObject.put("kafka_time", cr.timestamp() / 1000);
                jsonObject.put("kafka_time", cr.timestamp());
            } catch (Exception e) {
                log.error("错误的消息格式 msg = {}", cr.value());
//            log.error("错误的消息格式 msg = {}", msg);
                return;
            }

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String dt = simpleDateFormat.format(new Date(etLong));

//        Random random = new Random();
//        dt = dts[random.nextInt(dts.length)];

            try {

                TableTunnel.StreamUploadSession session = sessionManager.getSession(odpsConfiguration.getHuyaLiveLogTableName() + "&" + dt);
                Record record = session.newRecord();
                TableSchema schema = sessionManager.getTableSchema();
                for (int i = 0; i < schema.getColumns().size(); i++) {
                    Column column = schema.getColumn(i);

                    switch (column.getType()) {
                        case BIGINT:
                            record.set(i, jsonObject.getLongValue(column.getName()));
                            break;
                        case BOOLEAN:
                            record.set(i, jsonObject.getBooleanValue(column.getName()));
                            //                    record.setBoolean(i, false);
                            break;
                        case DATETIME:
                            record.set(i, jsonObject.getDate(column.getName()));
                            //                    record.setDatetime(i, new Date());
                            break;
                        case DOUBLE:
                            record.set(i, jsonObject.getDoubleValue(column.getName()));
                            //                    record.setDouble(i, 2000.25);
                            break;
                        case STRING:
                            record.setString(i, jsonObject.getString(column.getName()));
                            //                    record.setString(i, "aaaab");
                            break;

                        case INT:
                            record.set(i, jsonObject.getIntValue(column.getName()));
                            break;

                        default:
                            throw new RuntimeException("Unknown column type: " + column.getType());
                    }
                }
                TableTunnel.StreamRecordPack recordPack = sessionManager.getRecordPack(odpsConfiguration.getHuyaLiveLogTableName(), dt);
                recordPack.append(record);
            } catch (TunnelException | IOException e) {
                log.error("写入错误 data = {} , Exception = {} ", cr.value(), e);
            }
        }
    }
}
