package com.example.streamtunneldemo1;

import java.io.IOException;
import java.util.Date;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;

public class StreamUploadSample {
    // 阿里云账号AccessKey ID。
    private static String accessId = "<your_access_id>";
    // 阿里云账号AccessKey Secret。
    private static String accessKey = "<your_access_key>";
    // MaxCompute项目的Endpoint信息，详情请参见配置Endpoint。
    private static String odpsEndpoint = "<endpoint>";
    // MaxCompute项目的Tunnel Endpoint信息，详情请参见配置Endpoint。
    private static String tunnelEndpoint = "<tunnel_endpoint>";
    // MaxCompute项目的名称。
    private static String project = "<your_project>";
    // MaxCompute项目中的表名称。
    private static String table = "<your_table_name>";
    // MaxCompute项目中的表的分区信息。
    private static String partition = "<your_partition_spec>";

    public static void main(String args[]) {
        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setEndpoint(odpsEndpoint);
        odps.setDefaultProject(project);
        try {
            TableTunnel tunnel = new TableTunnel(odps);
            // 默认情况下，不需要设置Tunnel Endpoint，可以通过Endpoint自动路由。
            // 只有少数场景（比如路由的Endpoint网络不通）需要设置Tunnel Endpoint。
            // 可以通过以下接口设置Tunnel Endpoint。
            // tunnel.setEndpoint(tunnelEndpoint);
            PartitionSpec partitionSpec = new PartitionSpec(partition);


            TableTunnel.StreamUploadSession uploadSession = tunnel.createStreamUploadSession(project,
                    table, partitionSpec);
            TableSchema schema = uploadSession.getSchema();  //  通过  odps.tables() 获取

            TableTunnel.StreamRecordPack pack = uploadSession.newRecordPack();

            Record record = uploadSession.newRecord();

            for (int i = 0; i < schema.getColumns().size(); i++) {
                Column column = schema.getColumn(i);
                switch (column.getType()) {
                    case BIGINT:
                        record.setBigint(i, 1L);
                        break;
                    case BOOLEAN:
                        record.setBoolean(i, true);
                        break;
                    case DATETIME:
                        record.setDatetime(i, new Date());
                        break;
                    case DOUBLE:
                        record.setDouble(i, 0.0);
                        break;
                    case STRING:
                        record.setString(i, "sample");
                        break;
                    default:
                        throw new RuntimeException("Unknown column type: "
                                + column.getType());
                }
            }

            //
            for (int i = 0; i < 10; i++) {
                pack.append(record);
            }

            int retry = 0;
            while (retry < 3) {
                try {
                    // flush成功表示数据写入成功，写入成功后数据立即可见。
                    // flush成功后pack对象可以复用，避免频繁申请内存导致内存回收。
                    // flush失败可以直接重试。
                    // flush失败后pack对象不可重用，需要重新创建新的StreamRecordPack对象。
                    String traceId = pack.flush();
                    System.out.println("flush success:" + traceId);
                    break;
                } catch (IOException e) {
                    e.printStackTrace();
                    Thread.sleep(500);
                }
            }

            System.out.println("upload success!");
        } catch (TunnelException e) {
            e.printStackTrace();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
