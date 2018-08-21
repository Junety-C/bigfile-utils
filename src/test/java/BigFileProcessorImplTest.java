import cn.junety.tools.bigfile.BigFileProcessor;
import cn.junety.tools.bigfile.config.BigFileConfig;
import cn.junety.tools.bigfile.row.RowHandler;

import java.io.IOException;

/**
 * Created by caijt on 2018/8/21
 */
public class BigFileProcessorImplTest {

    public static void main(String[] args) throws IOException {
        /*
            data format: {md5},{number}

            c77d5d4c907323260c0b702124804e47,12798332
            63d69bbcd8e7962878c8e0f671fa3ff3,1231267
            ...
         */
        BigFileProcessor bigFileProcessor = BigFileProcessor.create(
                new BigFileConfig
                        .Builder()
                        .setBaseDirectory("/Users/caijt/Desktop/bigfile-test/bigfile/")
                        .build(),
                new RowHandler() {
                    @Override
                    public int hash(String key) {
                        return key.hashCode();
                    }

                    @Override
                    public String getKey(String row) {
                        String[] part = row.split(",");
                        return part[0];
                    }

                    @Override
                    public String clash(String row1, String row2) {
                        String[] part1 = row1.split(",");
                        String[] part2 = row2.split(",");
                        return part1[1] + "," + part2[1];
                    }

                    @Override
                    public boolean accept(String row) {
                        return row != null && row.length() > 0 && row.split(",").length == 2;
                    }
                });
    }
}
