package cn.junety.tools.bigfile.processor;

import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.FileUtils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by caijt on 2018/8/17
 */
public class ShardingBufferWriter {

    private static final long KB = 1024;
    private static final long MB = 1024 * KB;

    private static final long DEFAULT_BUFFER_MAX_SIZE = 256 * KB;
    private static final long DEFAULT_BUFFER_LIST_MAX_SIZE = 8 * MB;

    // 分片的数量
    private final int shardingCount;

    // 每个分片对应的文件路径
    private final String[] shardingFileList;

    // buffer list
    private final Buffer[] bufferList;

    // 当前buffer list存储的字节大小
    private long bufferListSize;

    // 每个buffer的字节大小上限, 超过则同步数据到文件
    private final long bufferMaxSize;

    // buffer list总的字节大小上限, 超过则选择一些buffer的数据进行落盘
    private final long bufferListMaxSize;

    // 数据项处理逻辑
    private final RowHandler rowHandler;

    public ShardingBufferWriter(List<String> shardingFileList, RowHandler rowHandler) {
        if (shardingFileList == null || shardingFileList.isEmpty()) {
            throw new IllegalArgumentException("sharding file list is null or empty...");
        }
        this.shardingCount = shardingFileList.size();
        this.shardingFileList = shardingFileList.toArray(new String[0]);
        this.bufferListSize = 0;
        this.bufferList = initBufferList(shardingCount);
        this.bufferMaxSize = DEFAULT_BUFFER_MAX_SIZE;
        this.bufferListMaxSize = initBufferListMaxSize();
        this.rowHandler = rowHandler;
    }

    private Buffer[] initBufferList(int size) {
        Buffer[] buffers = new Buffer[size];
        for (int i = 0; i < size; i++) {
            buffers[i] = new Buffer();
        }
        return buffers;
    }

    private long initBufferListMaxSize() {
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        MemoryUsage headMemory = memory.getHeapMemoryUsage();
        return Math.max(headMemory.getMax() / 75, DEFAULT_BUFFER_LIST_MAX_SIZE);
    }

    void write(String row) throws IOException {
        if (rowHandler.accept(row)) {
            int index = Math.abs(rowHandler.hash(rowHandler.getKey(row)) % shardingCount);
            bufferListSize += bufferList[index].add(row);

            ensureBufferSize(index);
        }
    }

    void flush() throws IOException {
        for (int i = 0; i < shardingCount; i++) {
            flushBuffer(i);
        }
    }

    private void ensureBufferSize(int index) throws IOException {
        // buffer超过阈值, 则把buffer里的数据同步到文件
        if (bufferList[index].size >= bufferMaxSize) {
            flushBuffer(index);
        } else if (bufferListSize >= bufferListMaxSize) {
            flushBuffer(findLargestBuffer());
        }
    }

    private int findLargestBuffer() {
        int index = 0;
        Buffer targetBuffer = bufferList[index];
        for (int i = 1; i < bufferList.length; i++) {
            Buffer currentBuffer = bufferList[i];
            if (currentBuffer.getSize() > targetBuffer.getSize()) {
                targetBuffer = currentBuffer;
                index = i;
            }
        }
        return index;
    }

    private void flushBuffer(int index) throws IOException {
        Buffer buffer = bufferList[index];
        bufferListSize -= buffer.getSize();
        FileUtils.writeLines(shardingFileList[index], buffer.getRows(), true);
        bufferList[index] = new Buffer();
    }

    private class Buffer {
        private List<String> rows;
        private long size;

        Buffer() {
            this.rows = new ArrayList<>(1024);
            this.size = 0;
        }

        long add(String row) {
            rows.add(row);
            size += row.length();
            return row.length();
        }

        List<String> getRows() {
            return rows;
        }

        long getSize() {
            return size;
        }
    }
}
