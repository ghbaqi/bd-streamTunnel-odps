package com.example.streamtunneldemo1;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RecordPackDecorator implements TableTunnel.StreamRecordPack {

    private TableTunnel.StreamRecordPack recordPack;
    private Lock lock;

    public RecordPackDecorator(TableTunnel.StreamRecordPack recordPack) {
        this.recordPack = recordPack;
        lock = new ReentrantLock();
    }

    @Override
    public void append(Record record) throws IOException {
        lock.lock();
        try {
            recordPack.append(record);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getRecordCount() {
        return recordPack.getRecordCount();
    }

    @Override
    public long getDataSize() {
        return recordPack.getDataSize();
    }

    @Override
    public String flush() throws IOException {
        lock.lock();
        try {
            return recordPack.flush();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TableTunnel.FlushResult flush(TableTunnel.FlushOption flushOption) throws IOException {
        return recordPack.flush(flushOption);
    }
}
