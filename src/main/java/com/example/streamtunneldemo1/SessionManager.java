package com.example.streamtunneldemo1;

import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class SessionManager {


    @Autowired
    private OdpsConfiguration configuration;

    @Autowired
    private ScheduledThreadPoolExecutor scheduledThreadPool;

    TableTunnel tunnel;
    Odps odps;

    private TableSchema schema;

    public TableSchema getTableSchema() {
        return schema;
    }

//    private Map<String, TableSchema> tableSchemaMap = new HashMap<>();


    @PostConstruct
    public void init() throws TunnelException, IOException {


        Account account = new AliyunAccount(configuration.getAccessId(), configuration.getAccessKey());
        odps = new Odps(account);
        odps.setEndpoint(configuration.getOdpsEndpoint());
        odps.setDefaultProject(configuration.getProject());
        tunnel = new TableTunnel(odps);

        schema = odps.tables().get(configuration.getHuyaLiveLogTableName()).getSchema();

        sessionLock = new ReentrantLock();
        recordPackLock = new ReentrantLock();

        // 默认情况下，不需要设置Tunnel Endpoint，可以通过Endpoint自动路由。
        // 只有少数场景（比如路由的Endpoint网络不通）需要设置Tunnel Endpoint。
        // 可以通过以下接口设置Tunnel Endpoint。
        // tunnel.setEndpoint(tunnelEndpoint);
//        PartitionSpec partitionSpec = new PartitionSpec(partition);
//        TableTunnel.StreamUploadSession uploadSession = tunnel.createStreamUploadSession(project,
//                table, partitionSpec);
//        TableSchema schema = uploadSession.getSchema();  //  通过  odps.tables() 获取
//        TableTunnel.StreamRecordPack pack = uploadSession.newRecordPack();
//        Record record = uploadSession.newRecord();
    }

    private Map<String, TableTunnel.StreamUploadSession> sessionMap = new ConcurrentHashMap<>();

    private Map<String, RecordPackDecorator> recordPackMap = new ConcurrentHashMap<>();


    private Lock sessionLock;

    /**
     * session 的维护和获取。
     * session 没有使用时间限制 , 但是也要注意 创建 太多 可能会导致 oom
     *
     * @return
     */
    public TableTunnel.StreamUploadSession getSession(String key) throws TunnelException {
        if (!StringUtils.hasLength(key) || !key.contains("&")) {
            throw new RuntimeException("非法参数1");
        }

        String tName = key.split("&")[0];
        String dt = key.split("&")[1];
        if (!StringUtils.hasLength(tName) || !StringUtils.hasLength(dt)) {
            throw new RuntimeException("非法参数2");
        }

        TableTunnel.StreamUploadSession exitSession;
        sessionLock.lock();
        try {
            exitSession = sessionMap.get(key);
            if (exitSession != null) {
                return exitSession;
            }
            // 创建新的 session
            PartitionSpec partitionSpec = new PartitionSpec();
            partitionSpec.set("dt", dt);
            TableTunnel.StreamUploadSession uploadSession = tunnel.createStreamUploadSession(configuration.getProject(),
                    tName, partitionSpec, true);
            sessionMap.put(key, uploadSession);

            // session 可以一直使用 。 定期移除  防止 oom
            scheduledThreadPool.schedule(new Runnable() {
                @Override
                public void run() {

                    recordPackLock.lock();
                    try {
                        log.info("session 定期进行移除 key = {}", key);
                        log.info("sessionMap size = {} , recordPackMap size = {} ", sessionMap.size(), recordPackMap.size());
                        sessionMap.remove(key);
                        TableTunnel.StreamRecordPack pack = recordPackMap.get(key);

                        if (pack != null) {
                            try {
                                recordPackMap.remove(key);   // 会造成下面 get 的时候产生 空指针
                                pack.flush();
                            } catch (IOException e) {
                                log.error("flush 失败1 e = {}", e);
                            }
                        }
                    } finally {
                        recordPackLock.unlock();
                    }

                }
            }, configuration.getSessionLifeMinute(), TimeUnit.MINUTES);
            return uploadSession;
        } finally {
            sessionLock.unlock();
        }
    }


    private Lock recordPackLock;

    /**
     * 获取   StreamRecordPack  写入对象
     * append是非线程安全的
     * 这个接口是写内存，单线程写性能上是有保证的
     * <p>
     * 写入达到一定条数进行 flush 且 一定时间后也进行 flush。
     *
     * @param tName
     * @param dt
     * @return
     * @throws TunnelException
     * @throws IOException
     */
    public RecordPackDecorator getRecordPack(String tName, String dt) throws TunnelException, IOException {

        recordPackLock.lock();
        try {
            String key = tName + "&" + dt;
            RecordPackDecorator exitPack = recordPackMap.get(key);
            if (exitPack == null) {
                // 新创建 StreamRecordPack
                TableTunnel.StreamUploadSession session = getSession(key);
                TableTunnel.StreamRecordPack pack = session.newRecordPack(new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY, 5, 2));
                recordPackMap.put(key, new RecordPackDecorator(pack));
//            scheduledThreadPool.schedule(new RecordPackFlushTask(key), 20, TimeUnit.SECONDS);

            } else {
                if (exitPack.getRecordCount() >= configuration.getBatchSize()) {
                    int result = flush(exitPack, key);
                    // flush  失败
                    if (result < 0) {
                        TableTunnel.StreamUploadSession session = getSession(key);
                        TableTunnel.StreamRecordPack pack = session.newRecordPack(new CompressOption(CompressOption.CompressAlgorithm.ODPS_SNAPPY, 5, 2));
                        recordPackMap.put(key, new RecordPackDecorator(pack));
                    }
                }
            }

            return recordPackMap.get(key);
        } finally {
            recordPackLock.unlock();
        }


    }

    private int flush(TableTunnel.StreamRecordPack recordPack, String key) throws IOException, TunnelException {

        int retry = 0;
        while (retry < 3)
            try {
                // flush成功表示数据写入成功，写入成功后数据立即可见。
                // flush成功后pack对象可以复用，避免频繁申请内存导致内存回收。
                // flush失败可以直接重试。

                log.info("begin flush recordCount = {} , dataSize = {}", recordPack.getRecordCount(), recordPack.getDataSize());
                retry++;
                recordPack.flush();
                log.info("flush success retry = {}", retry);
                return 1;
            } catch (IOException e) {
                if (retry == 3) {
                    // 最后一次 flush 出了异常 。需要重新创建  RecordPack 对象 。 TODO 且这里丢失了  一部分数据
                    // flush失败后pack对象不可重用，需要重新创建新的StreamRecordPack对象。
                    log.error("exitPack.flush 最终失败 size = {} , dataLength = {}", recordPack.getRecordCount(), recordPack.getDataSize());
                    return -1;
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        log.error("thread  error  {}", ex);
                    }
                }

            }
        return -1;
    }

}
