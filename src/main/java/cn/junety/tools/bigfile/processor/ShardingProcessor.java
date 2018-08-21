package cn.junety.tools.bigfile.processor;

import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.FileUtils;
import cn.junety.tools.bigfile.utils.LineIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by caijt on 2018/8/17
 */
public class ShardingProcessor {

    private final String sourceFilePath;
    private final int shardingCount;
    private final String shardingDirectory;
    private final RowHandler rowHandler;
    private final List<String> shardingFileList;

    public ShardingProcessor(String shardingDirectory, String sourceFilePath, int shardingCount, RowHandler rowHandler) {
        this.sourceFilePath = sourceFilePath;
        this.shardingCount = shardingCount;
        this.shardingDirectory = shardingDirectory;
        this.rowHandler = rowHandler;
        this.shardingFileList = new ArrayList<>(shardingCount);
    }

    // 对源文件进行切分
    public List<String> sharding() throws IOException {
        List<String> shardingFileList = createFiles();
        ShardingBufferWriter bufferWriter = new ShardingBufferWriter(shardingFileList, rowHandler);

        try (LineIterator lineIterator = FileUtils.newLineIterator(sourceFilePath)) {
            while (lineIterator.hasNext()) {
                bufferWriter.write(lineIterator.nextLine());
            }
            bufferWriter.flush();
        }
        return shardingFileList;
    }

    // 删除分片文件和临时目录
    public void clean() throws IOException {
        if (!shardingFileList.isEmpty()) {
            for (String filePath : shardingFileList) {
                FileUtils.deleteIfExists(filePath);
            }
        }
    }

    // 创建每个分片文件
    private List<String> createFiles() throws IOException {
        shardingFileList.clear();
        String fileName = FileUtils.getName(sourceFilePath);
        for (int i = 0; i < shardingCount; i++) {
            String shardingFilePath = shardingDirectory + "/" + getShardingFileName(fileName, i);
            if (!FileUtils.exist(shardingFilePath)) {
                FileUtils.createFile(shardingFilePath);
            }
            shardingFileList.add(shardingFilePath);
        }
        return shardingFileList;
    }

    private String getShardingFileName(String fileName, int index) {
        return String.format("%s-sharding-%s", fileName, index);
    }
}
