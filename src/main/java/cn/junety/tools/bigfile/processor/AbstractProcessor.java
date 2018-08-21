package cn.junety.tools.bigfile.processor;

import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.FileUtils;
import cn.junety.tools.bigfile.utils.LineIterator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by caijt on 2018/8/17
 */
public class AbstractProcessor {

    protected BigFileConfig config;
    protected RowHandler rowHandler;

    public AbstractProcessor(BigFileConfig config, RowHandler rowHandler) {
        this.config = config;
        this.rowHandler = rowHandler;
    }

    /**
     * 对源文件进行分片
     *
     * @param sourceFilePath 源文件路径
     * @param shardingCount  切分的分片数
     */
    public List<String> sharding(String sourceFilePath, int shardingCount) throws IOException {
        return createShardingProcessor(createTempDirectory(), sourceFilePath, shardingCount).sharding();
    }

    /**
     * 使用归并排序对分片文件进行合并
     *
     * @param shardingFileList 分片文件的路径列表
     * @param outputFilePath   输出的文件路径
     */
    public long sort(List<String> shardingFileList, String outputFilePath) throws IOException {
        long totalSize = 0;

        // 先对小文件进行内部排序
        for (String filePath : shardingFileList) {
            List<String> rowList = FileUtils.readLines(filePath);
            totalSize += rowList.size();
            Collections.sort(rowList);
            FileUtils.writeLines(filePath, rowList, false);
        }

        // 每次拿两个文件进行归并排序
        int step = 1;
        do {
            step *= 2;
            for (int i = 0; i < shardingFileList.size(); i += step) {
                if (i + step / 2 >= shardingFileList.size()) continue;

                String file1 = shardingFileList.get(i);
                String file2 = shardingFileList.get(i + step / 2);
                String file3 = file1 + ".tmp";

                sort(file1, file2, file3);

                FileUtils.delete(file1);
                FileUtils.delete(file2);
                FileUtils.rename(file3, file1);
            }
        } while (step < shardingFileList.size());

        FileUtils.rename(shardingFileList.get(0), outputFilePath, true);

        return totalSize;
    }

    /**
     * 对两个有序文件通过使用归并排序得到一个有序的文件
     *
     * @param inputFilePath1 有序文件1的路径
     * @param inputFilePath2 有序文件2的路径
     * @param outputFilePath 排序后的文件路径
     */
    private void sort(String inputFilePath1, String inputFilePath2, String outputFilePath) throws IOException {
        FileUtils.deleteIfExists(outputFilePath);
        try (BufferedReader br1 = Files.newBufferedReader(Paths.get(inputFilePath1));
             BufferedReader br2 = Files.newBufferedReader(Paths.get(inputFilePath2));
             BufferedWriter bw = Files.newBufferedWriter(Paths.get(outputFilePath), StandardOpenOption.CREATE_NEW)) {

            String str1 = br1.readLine();
            String str2 = br2.readLine();
            while (str1 != null && str2 != null) {
                int comp = str1.compareTo(str2);
                if (comp < 0) {
                    bw.write(str1);
                    bw.newLine();
                    str1 = br1.readLine();
                } else {
                    bw.write(str2);
                    bw.newLine();
                    str2 = br2.readLine();
                }
            }

            while (str1 != null) {
                bw.write(str1);
                bw.newLine();
                str1 = br1.readLine();
            }

            while (str2 != null) {
                bw.write(str2);
                bw.newLine();
                str2 = br2.readLine();
            }
        }
    }

    /**
     * 读取文件作为一个集合, 使用rowHandler解决数据冲突
     *
     * @param filePath 文件路径
     */
    Set<String> readAsSet(String filePath) throws IOException {
        try (LineIterator lineIterator = FileUtils.newLineIterator(filePath)) {

            Map<String, String> rowSet = new HashMap<>();
            String newRow, oldRow, key;

            while (lineIterator.hasNext()) {
                newRow = lineIterator.nextLine();
                key = rowHandler.getKey(newRow);
                oldRow = rowSet.get(key);
                if (oldRow == null) {
                    rowSet.put(key, newRow);
                } else {
                    rowSet.put(key, rowHandler.clash(newRow, oldRow));
                }
            }
            return new HashSet<>(rowSet.values());
        }
    }

    ShardingProcessor createShardingProcessor(String tempDirectory, String sourceFilePath, int shardingCount) {
        return new ShardingProcessor(tempDirectory, sourceFilePath,shardingCount, rowHandler);
    }

    String createTempDirectory() throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        String directory = String.format("%s%s-%s/", config.getBaseDirectory(),
                dateFormat.format(new Date()),
                String.format("%04d", new Random().nextInt(10000)));
        FileUtils.createDirectories(directory);
        return directory;
    }

    int calcShardingCount(String... sourceFiles) {
        long maxLength = 0;
        for (String filePath : sourceFiles) {
            long length = FileUtils.getFileLength(filePath);
            maxLength = Math.max(maxLength, length);
        }
        int count = (int) ((maxLength + config.getShardingFileSize() - 1) / config.getShardingFileSize());
        return count < 1 ? 1 : count;
    }
}
