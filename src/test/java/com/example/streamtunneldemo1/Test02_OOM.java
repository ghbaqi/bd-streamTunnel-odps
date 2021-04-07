package com.example.streamtunneldemo1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.omg.CORBA.PUBLIC_MEMBER;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;

/**
 * 使用线程池   排查 oom 问题
 */
@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
public class Test02_OOM {

    @Autowired
    private HuyaListener listener;

    @Test
    public void t1() throws Exception {
        log.info("1111----------");

        String msg = "{\"EventType\":\"ecdnl-access-log\",\"QoSType\":\"ecdnv4\",\"Timestamp\":\"2021-04-02 14:30:33 +0800\",\"_lsvc_time\":1617345034,\"app_id\":\"huya-slice\",\"app_name\":\"huyalive\",\"body_bytes_sent\":49838,\"bytes_sent\":49838,\"cdn_user_agent\":\"cdn_agent_ppio\",\"hit_info\":\"TCP_HIT -\",\"host\":\"psource.huya.com\",\"hostname\":\"1424e603\",\"http_referer\":\"\",\"http_user_agent\":\"android, 20000313_APP(adr\\u00268.12.2\\u0026huawei\\u002629)\",\"huya_uid\":\"H_3402589098_Y\",\"ip\":\"183.93.250.142\",\"is_complete\":true,\"isp\":\"联通\",\"kj_id\":\"1424e603\",\"province\":\"湖北省\",\"region\":\"华东\",\"remote_addr\":\"58.240.167.138\",\"request\":\"GET /huyalive/1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16.pstream?from=ppio\\u0026uid=H_3402589098_Y\\u0026wsSecret=a061328868f61ed4b7d1f549e3fdc4d7\\u0026wsTime=6066b9fa\\u0026line=7\\u0026isUsePsdk=0\\u0026isTestEnv=0\\u0026hotgather=0\\u0026ctype=huya_adr\\u0026txyp=o:cs6;\\u0026fs=bgct\\u0026\\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\\u0026sphdDC=huya\\u0026sphd=264_*-265_*\\u0026baseIndex=6173318563006448\\u0026u=5999874663\\u0026t=2 HTTP/1.1\",\"request_at\":1617345020025,\"request_id\":\"3ddfb487-e1a9-4193-9bf6-4bf83efe3add\",\"request_time\":13.022,\"scheme\":\"HTTP\",\"scheme_hdr\":\"http\",\"scheme_sup\":\"http\",\"status\":200,\"stream_name\":\"1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16\",\"unix_timestamp\":1617345033022,\"upstream_addr\":\"-\",\"upstream_response_time\":12.997,\"url\":\"http://183.93.250.142:35711/huyalive/1677942375-1677942375-7206707625197568000-3356008206-10057-A-0-1_405_0_66@7@16.pstream?from=ppio\\u0026uid=H_3402589098_Y\\u0026wsSecret=a061328868f61ed4b7d1f549e3fdc4d7\\u0026wsTime=6066b9fa\\u0026line=7\\u0026isUsePsdk=0\\u0026isTestEnv=0\\u0026hotgather=0\\u0026ctype=huya_adr\\u0026txyp=o:cs6;\\u0026fs=bgct\\u0026\\u0026sphdcdn=al_7-tx_3-js_3-ws_7-bd_2-hw_2\\u0026sphdDC=huya\\u0026sphd=264_*-265_*\\u0026baseIndex=6173318563006448\\u0026u=5999874663\\u0026t=2\",\"uuid\":\"994dc817-6928-4c05-aa22-7eef2ddb8e26\",\"version\":\"v0.3.13\",\"vhost\":\"hy.ppio-cloud.com\"}\n";
        ConsumerRecord<String, String> cr = new ConsumerRecord<String, String>("t1", 1, 555555L, "k1", msg);
        ArrayList<ConsumerRecord<String, String>> crs = new ArrayList<>(128);
        for (int i = 0; i < 100; i++) {
            crs.add(cr);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 40000; i++) {
            listener.listen(crs);
        }
        long end = System.currentTimeMillis();
        System.out.println("花费时间 s = " + (end - start) / 1000);
        Thread.sleep(999999999999L);
    }
}
