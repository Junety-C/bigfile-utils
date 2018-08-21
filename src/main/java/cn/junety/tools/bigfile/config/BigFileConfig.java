package cn.junety.tools.bigfile.config;

import java.lang.management.ManagementFactory;

/**
 * Created by caijt on 2018/8/17
 */
public class BigFileConfig {

    private static final long KB = 1024;
    private static final long MB = 1024 * KB;


    private static final long DEFAULT_SHARDING_FILE_SIZE = 8 * MB;

    private static final long DEFAULT_MAX_SHARDING_BUFFER_ROWS_IN_MEMORY = 1_000_000;

    private static final long DEFAULT_MAX_SHARDING_BUFFER_SIZE_IN_MEMORY =
            Math.max(ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax()/75, DEFAULT_SHARDING_FILE_SIZE);

    private static final String DEFAULT_BASE_DIRECTORY = System.getProperty("user.dir") + "/bigfile/";


    private final String baseDirectory;
    // 每个分片文件的期望大小
    private final long shardingFileSize;
    // 分片的buffer在内存中能驻留的最大数据条目
    private final long maxShardingBufferRowsInMemory;
    // 分片的buffer在内存中能驻留的最大字节数
    private final long maxShardingBufferSizeInMemory;

    public BigFileConfig(Builder builder) {
        this.baseDirectory = builder.baseDirectory;
        this.shardingFileSize = builder.shardingFileSize;
        this.maxShardingBufferRowsInMemory = builder.maxShardingBufferRowsInMemory;
        this.maxShardingBufferSizeInMemory = builder.maxShardingBufferSizeInMemory;
    }


    public String getBaseDirectory() {
        return baseDirectory;
    }

    public long getShardingFileSize() {
        return shardingFileSize;
    }

    public long getMaxShardingBufferRowsInMemory() {
        return maxShardingBufferRowsInMemory;
    }

    public long getMaxShardingBufferSizeInMemory() {
        return maxShardingBufferSizeInMemory;
    }


    public static class Builder {

        private String baseDirectory = DEFAULT_BASE_DIRECTORY;
        private long shardingFileSize = DEFAULT_SHARDING_FILE_SIZE;
        private long maxShardingBufferRowsInMemory = DEFAULT_MAX_SHARDING_BUFFER_ROWS_IN_MEMORY;
        private long maxShardingBufferSizeInMemory = DEFAULT_MAX_SHARDING_BUFFER_SIZE_IN_MEMORY;

        public Builder setBaseDirectory(String baseDirectory) {
            this.baseDirectory = baseDirectory;
            return this;
        }

        public Builder setShardingFileSize(long shardingFileSize) {
            this.shardingFileSize = shardingFileSize;
            return this;
        }

        public Builder setMaxShardingBufferRowsInMemory(long maxShardingBufferRowsInMemory) {
            this.maxShardingBufferRowsInMemory = maxShardingBufferRowsInMemory;
            return this;
        }

        public Builder setMaxShardingBufferSizeInMemory(long maxShardingBufferSizeInMemory) {
            this.maxShardingBufferSizeInMemory = maxShardingBufferSizeInMemory;
            return this;
        }

        public BigFileConfig build() {
            return new BigFileConfig(this);
        }
    }
}
