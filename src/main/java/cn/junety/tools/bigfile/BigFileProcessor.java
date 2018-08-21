package cn.junety.tools.bigfile;

import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.row.DefaultRowHandler;
import cn.junety.tools.bigfile.row.RowHandler;

import java.io.IOException;
import java.util.List;

/**
 * Created by caijt on 2018/8/17
 */
public interface BigFileProcessor {

    /**
     * 使用默认的 rowHandler 实例化 BigFileProcessorImpl
     *
     * @param config 配置信息
     */
    static BigFileProcessor create(BigFileConfig config) {
        return create(config, new DefaultRowHandler());
    }

    /**
     * 使用自定义的 rowHandler 实例化 BigFileProcessorImpl
     *
     * @param config 配置信息
     */
    static BigFileProcessor create(BigFileConfig config, RowHandler rowHandler) {
        return new BigFileProcessorImpl(config, rowHandler);
    }

    /**
     * 计算文件1和文件2的交集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException;

    /**
     * 计算文件1和文件2的交集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException;

    /**
     * 计算文件1和文件2的并集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException;

    /**
     * 计算文件1和文件2的并集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException;

    /**
     * 计算文件1减去文件2的差集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException;

    /**
     * 计算文件1减去文件2的差集
     *
     * @param sourceFilePath1 源文件路径1
     * @param sourceFilePath2 源文件路径2
     * @param outputFilePath  输出文件路径
     */
    long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException;

    /**
     * 文件排序(字典序)
     *
     * @param sourceFilePath 源文件路径
     * @param outputFilePath 输出文件路径
     */
        long sort(String sourceFilePath, String outputFilePath) throws IOException;

    /**
     * 过滤不合法数据
     *
     * @param sourceFilePath 源文件路径
     * @param outputFilePath 输出文件路径
     */
    long filter(String sourceFilePath, String outputFilePath) throws IOException;

    /**
     * 文件数据分片
     *
     * @param sourceFilePath 源文件路径
     * @param shardingCount  分片数量
     */
    List<String> sharding(String sourceFilePath, int shardingCount) throws IOException;
}
