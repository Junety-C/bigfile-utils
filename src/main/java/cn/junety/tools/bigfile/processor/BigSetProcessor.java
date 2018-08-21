package cn.junety.tools.bigfile.processor;

import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.FileUtils;
import cn.junety.tools.bigfile.utils.LineIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by caijt on 2018/8/17
 */
@Slf4j
public class BigSetProcessor extends AbstractProcessor {

    public BigSetProcessor(BigFileConfig config, RowHandler rowHandler) {
        super(config, rowHandler);
    }

    /**
     * 求两个文件的交集, 输出到目标文件
     *
     * @param sourceFilePath1 源文件1
     * @param sourceFilePath2 源文件2
     * @param outputFilePath 目标文件
     * @param sort 是否需要对结果进行排序
     */
    public long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        return setCalculate(
                sourceFilePath1,
                sourceFilePath2,
                outputFilePath,
                (set1, set2) -> {
                    Map<String, String> dataSet = new HashMap<>(set1.size());
                    String oldRow, key;

                    for (String row : set2) {
                        dataSet.put(rowHandler.getKey(row), row);
                    }

                    Set<String> result = new HashSet<>(set1.size());
                    for (String newRow : set1) {
                        key = rowHandler.getKey(newRow);
                        oldRow = dataSet.get(key);
                        if (oldRow != null) {
                            result.add(rowHandler.clash(newRow, oldRow));
                        }
                    }
                    return result;
                },
                sort);
    }

    /**
     * 求两个文件的并集, 输出到目标文件
     *
     * @param sourceFilePath1 源文件1
     * @param sourceFilePath2 源文件2
     * @param outputFilePath 目标文件
     * @param sort 是否需要对结果进行排序
     */
    public long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        return setCalculate(
                sourceFilePath1,
                sourceFilePath2,
                outputFilePath,
                (set1, set2) -> {
                    Map<String, String> dataSet = new HashMap<>(set1.size());
                    String oldRow, key;

                    for (String row : set2) {
                        dataSet.put(rowHandler.getKey(row), row);
                    }

                    for (String newRow : set1) {
                        key = rowHandler.getKey(newRow);
                        oldRow = dataSet.get(key);
                        if (oldRow != null) {
                            dataSet.put(key, rowHandler.clash(newRow, oldRow));
                        } else {
                            dataSet.put(key, newRow);
                        }
                    }
                    return new HashSet<>(dataSet.values());
                },
                sort);
    }

    /**
     * 求两个文件的差集(A-B), 输出到目标文件
     *
     * @param sourceFilePath1 源文件1
     * @param sourceFilePath2 源文件2
     * @param outputFilePath 目标文件
     * @param sort 是否需要对结果进行排序
     */
    public long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        return setCalculate(
                sourceFilePath1,
                sourceFilePath2,
                outputFilePath,
                (set1, set2) -> {
                    Map<String, String> dataSet = new HashMap<>(set1.size());
                    String oldRow, key;

                    for (String row : set2) {
                        dataSet.put(rowHandler.getKey(row), row);
                    }

                    Set<String> result = new HashSet<>(set1.size());
                    for (String newRow : set1) {
                        key = rowHandler.getKey(newRow);
                        oldRow = dataSet.get(key);
                        if (oldRow == null) {
                            result.add(newRow);
                        }
                    }
                    return result;
                },
                sort);
    }

    /**
     * 对两个文件进行集合运算
     *
     * @param sourceFilePath1 源文件1
     * @param sourceFilePath2 源文件2
     * @param outputFilePath 目标文件
     * @param handler 集合运算处理
     * @param sort 是否需要对结果进行排序
     */
    private long setCalculate(String sourceFilePath1, String sourceFilePath2, String outputFilePath,
                              BigSetHandler handler, boolean sort) throws IOException {

        int shardingCount = calcShardingCount(sourceFilePath1, sourceFilePath2);
        String tempDirectory = createTempDirectory();

        // 文件1分片
        ShardingProcessor shardingProcessor1 =
                new ShardingProcessor(tempDirectory, sourceFilePath1, shardingCount, rowHandler);
        List<String> shardingFileList1 = shardingProcessor1.sharding();

        // 文件2分片
        ShardingProcessor shardingProcessor2 =
                new ShardingProcessor(tempDirectory, sourceFilePath2, shardingCount, rowHandler);
        List<String> shardingFileList2 = shardingProcessor2.sharding();

        // 拿分片文件做集合运算
        List<String> outputShardingFileList;
        outputShardingFileList = calcWithoutThreadPool(shardingFileList1, shardingFileList2, shardingCount,
                handler, outputFilePath, tempDirectory);

        long totalSize;
        if (sort) {
            // 排序并且合并输出
            totalSize = sort(outputShardingFileList, outputFilePath);
        } else {
            // 合并输出
            totalSize = mergeShardingFile(outputShardingFileList, outputFilePath);
        }
        FileUtils.delete(tempDirectory);

        return totalSize;
    }

    /**
     * 对两个分片文件列表相同下标的文件做集合运算
     *
     * @param shardingFileList1 分片文件列表1
     * @param shardingFileList2 分片文件列表2
     * @param shardingCount 分片数量
     * @param handler 集合运算处理
     * @param outputFilePath 目标文件
     * @param tempDirectory 临时目录路径
     */
    private List<String> calcWithoutThreadPool(List<String> shardingFileList1, List<String> shardingFileList2,
                                               int shardingCount, BigSetHandler handler, String outputFilePath,
                                               String tempDirectory) throws IOException {
        String outputFileName = FileUtils.getName(outputFilePath);
        List<String> outputShardingFileList = new ArrayList<>();
        for (int i = 0; i < shardingCount; i++) {
            String file1 = shardingFileList1.get(i);
            String file2 = shardingFileList2.get(i);

            Set<String> set1 = readAsSet(file1);
            Set<String> set2 = readAsSet(file2);
            Set<String> result = handler.handle(set1, set2);

            String shardingFilePath = tempDirectory + getShardingFileName(outputFileName, i);
            FileUtils.writeLines(shardingFilePath, result, false);
            outputShardingFileList.add(shardingFilePath);

            FileUtils.deleteIfExists(file1);
            FileUtils.deleteIfExists(file2);
        }
        return outputShardingFileList;
    }

    /**
     * 将分片文件合并成目标文件
     *
     * @param shardingFileList 分片文件列表
     * @param outputFilePath 目标文件路径
     */
    private long mergeShardingFile(List<String> shardingFileList, String outputFilePath) throws IOException {
        FileUtils.deleteIfExists(outputFilePath);

        long totalSize = 0;
        try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(outputFilePath))) {
            for (String filePath : shardingFileList) {
                try (LineIterator lineIterator = FileUtils.newLineIterator(filePath)) {
                    while (lineIterator.hasNext()) {
                        bw.write(lineIterator.nextLine());
                        bw.newLine();
                        totalSize++;
                    }
                }
                FileUtils.delete(filePath);
            }
        }
        return totalSize;
    }

    private String getShardingFileName(String fileName, int index) {
        return String.format("%s-set-%s", fileName, String.valueOf(index));
    }
}
