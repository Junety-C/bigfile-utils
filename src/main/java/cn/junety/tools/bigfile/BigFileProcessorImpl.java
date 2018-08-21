package cn.junety.tools.bigfile;

import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.processor.BigListProcessor;
import cn.junety.tools.bigfile.processor.BigSetProcessor;
import cn.junety.tools.bigfile.row.RowHandler;
import cn.junety.tools.bigfile.utils.TimeMeter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by caijt on 2018/8/20
 */
@Slf4j
public class BigFileProcessorImpl implements BigFileProcessor {

    private final BigListProcessor bigListProcessor;
    private final BigSetProcessor bigSetProcessor;

    public BigFileProcessorImpl(BigFileConfig config, RowHandler rowHandler) {
        this.bigListProcessor = new BigListProcessor(config, rowHandler);
        this.bigSetProcessor = new BigSetProcessor(config, rowHandler);
    }

    @Override
    public long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException {
        return intersect(sourceFilePath1, sourceFilePath2, outputFilePath, false);
    }

    @Override
    public long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        TimeMeter timeMeter = new TimeMeter();
        long totalSize = bigSetProcessor.intersect(sourceFilePath1, sourceFilePath2, outputFilePath, sort);
        log.debug("intersect total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return totalSize;
    }

    @Override
    public long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException {
        return union(sourceFilePath1, sourceFilePath2, outputFilePath, false);
    }

    @Override
    public long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        TimeMeter timeMeter = new TimeMeter();
        long totalSize = bigSetProcessor.union(sourceFilePath1, sourceFilePath2, outputFilePath, sort);
        log.debug("union total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return totalSize;
    }

    @Override
    public long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath) throws IOException {
        return differ(sourceFilePath1, sourceFilePath2, outputFilePath, false);
    }

    @Override
    public long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
            throws IOException {

        TimeMeter timeMeter = new TimeMeter();
        long totalSize = bigSetProcessor.differ(sourceFilePath1, sourceFilePath2, outputFilePath, sort);
        log.debug("differ total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return totalSize;
    }

    @Override
    public long sort(String sourceFilePath, String outputFilePath) throws IOException {
        TimeMeter timeMeter = new TimeMeter();
        long totalSize = bigListProcessor.sort(sourceFilePath, outputFilePath);
        log.debug("sort total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return totalSize;
    }

    @Override
    public long filter(String sourceFilePath, String outputFilePath) throws IOException {
        TimeMeter timeMeter = new TimeMeter();
        long totalSize = bigListProcessor.filter(sourceFilePath, outputFilePath);
        log.debug("filter total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return totalSize;
    }

    @Override
    public List<String> sharding(String sourceFilePath, int shardingCount) throws IOException {
        TimeMeter timeMeter = new TimeMeter();
        List<String> shardingFileList = bigListProcessor.sharding(sourceFilePath, shardingCount);
        log.debug("sharding total use {} seconds", timeMeter.getUsed(TimeUnit.SECONDS));
        return shardingFileList;
    }
}
