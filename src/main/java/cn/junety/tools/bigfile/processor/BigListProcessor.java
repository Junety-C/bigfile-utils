package cn.junety.tools.bigfile.processor;

import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.FileUtils;
import cn.junety.tools.bigfile.utils.LineIterator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

/**
 * Created by caijt on 2018/8/17
 */
public class BigListProcessor extends AbstractProcessor {

    public BigListProcessor(BigFileConfig config, RowHandler rowHandler) {
        super(config, rowHandler);
    }

    /**
     * 对文件按字典序进行排序
     *
     * @param sourceFilePath 源文件路径
     * @param outputFilePath 输出路径
     */
    public long sort(String sourceFilePath, String outputFilePath) throws IOException {
        int shardingCount = calcShardingCount(sourceFilePath);

        // 文件分片
        String tempDirectory = createTempDirectory();
        ShardingProcessor shardingProcessor =
                createShardingProcessor(tempDirectory, sourceFilePath, shardingCount);
        List<String> shardingFileList = shardingProcessor.sharding();

        // 排序
        long totalSize = sort(shardingFileList, outputFilePath);
        FileUtils.delete(tempDirectory);
        return totalSize;
    }

    /**
     * 去重
     *
     * @param sourceFilePath 源文件路径
     * @param outputFilePath 输出路径
     */
    public long unique(String sourceFilePath, String outputFilePath) throws IOException {
        int shardingCount = calcShardingCount(sourceFilePath);

        // 文件分片
        String tempDirectory = createTempDirectory();
        ShardingProcessor shardingProcessor =
                createShardingProcessor(tempDirectory, sourceFilePath, shardingCount);
        List<String> shardingFileList = shardingProcessor.sharding();

        long totalSize = 0;
        for (String filePath : shardingFileList) {
            Set<String> set = readAsSet(filePath);
            totalSize += set.size();
            FileUtils.writeLines(outputFilePath, set);
        }
        FileUtils.delete(tempDirectory);
        return totalSize;
    }

    /**
     * 过滤不合法的数据
     *
     * @param sourceFilePath 源文件路径
     * @param outputFilePath 输出路径
     */
    public long filter(String sourceFilePath, String outputFilePath) throws IOException {
        try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(outputFilePath));
             LineIterator lineIterator = FileUtils.newLineIterator(sourceFilePath)) {
            long totalSize = 0;
            String line;
            while (lineIterator.hasNext()) {
                line = lineIterator.nextLine();
                if (rowHandler.accept(line)) {
                    totalSize++;
                    bw.write(line);
                    bw.newLine();
                }
            }
            return totalSize;
        }
    }
}
