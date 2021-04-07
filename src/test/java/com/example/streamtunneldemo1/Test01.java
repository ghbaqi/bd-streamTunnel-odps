package com.example.streamtunneldemo1;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sound.midi.Soundbank;
import java.sql.SQLOutput;

@SpringBootTest
@RunWith(SpringRunner.class)
public class Test01 {

    @Autowired
    private HuyaListener listener;

    String msg = "{\"EventType\":\"ecdnl-access-log\",\"QoSType\":\"ecdnv4\",\"Timestamp\":\"2021-04-02 14:30:33 +0800\",\"_lsvc_time\":1617345034,\"app_id\":\"huya-slice\",\"app_name\":\"huyalive\",\"body_bytes_sent\":221785,\"bytes_sent\":221785,\"cdn_user_agent\":\"cdn_agent_ppio\",\"hit_info\":\"TCP_HIT -\",\"host\":\"psource.huya.com\",\"hostname\":\"1424e603\",\"http_referer\":\"\",\"http_user_agent\":\"HYSDK(iOS, 30000002)_APP(ios\\u00268.12.0\\u0026store\\u002614.4.2)\",\"huya_uid\":\"H_4675609904_Y\",\"ip\":\"183.93.250.142\",\"is_complete\":true,\"isp\":\"联通\",\"kj_id\":\"1424e603\",\"province\":\"湖北省\",\"region\":\"华东\",\"remote_addr\":\"221.12.121.168\",\"request\":\"GET /huyalive/1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16.pstream?from=ppio\\u0026uid=H_4675609904_Y\\u0026wsSecret=8c75b0a8a3d2d2e37d1b85f3e91324a1\\u0026wsTime=6066b9f3\\u0026line=3\\u0026isUsePsdk=0\\u0026isTestEnv=0\\u0026hotgather=0\\u0026ctype=huya_ios\\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\\u0026sphdDC=huya\\u0026sphd=264_*-265_*\\u0026fs=gctex\\u0026baseIndex=6173270974838593\\u0026u=827099247499\\u0026t=3 HTTP/1.1\",\"request_at\":1617345012254,\"request_id\":\"ddf9abcd-98fd-4450-b40c-86ec4b305ee6\",\"request_time\":21.276,\"scheme\":\"HTTP\",\"scheme_hdr\":\"http\",\"scheme_sup\":\"http\",\"status\":200,\"stream_name\":\"1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16\",\"unix_timestamp\":1617345033276,\"upstream_addr\":\"-\",\"upstream_response_time\":21.022,\"url\":\"http://183.93.250.142:35711/huyalive/1271256459-1271256459-5460004916233764864-2542636374-10057-A-0-1-imgplus_420_0_66@0@16.pstream?from=ppio\\u0026uid=H_4675609904_Y\\u0026wsSecret=8c75b0a8a3d2d2e37d1b85f3e91324a1\\u0026wsTime=6066b9f3\\u0026line=3\\u0026isUsePsdk=0\\u0026isTestEnv=0\\u0026hotgather=0\\u0026ctype=huya_ios\\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\\u0026sphdDC=huya\\u0026sphd=264_*-265_*\\u0026fs=gctex\\u0026baseIndex=6173270974838593\\u0026u=827099247499\\u0026t=3\",\"uuid\":\"7fa6e818-9cbd-426f-b18b-160d5bb77713\",\"version\":\"v0.3.13\",\"vhost\":\"hy.ppio-cloud.com\"}";

    @Test
    public void t1() throws InterruptedException {
        long start = System.currentTimeMillis();

        System.out.println("*************  start  write  ************** ");
        for (int i = 0; i < 10000L * 500; i++) {
//            listener.listen(msg);
        }
        long delt = System.currentTimeMillis() - start;
        System.out.println("----------------------  finish minute = " + delt / 1000 / 60.0);
        Thread.sleep(10000000L);
    }


}
