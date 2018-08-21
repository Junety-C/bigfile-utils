## 概述

用于处理大文件的交并差集和排序。

主要思想是把一个大文件的每行数据按照哈希规则分散到多个小文件里去，然后再进行计算，防止OOM。

## 使用

使用方法：

```text
BigFileProcessor bigFileProcessor = BigFileProcessor.create(new BigFileConfig.Builder().build());
bigFileProcessor.sort("input", "output");
```

## 支持的功能

1. 求两个文件的交集

```text
long intersect(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
```

2. 求两个文件的并集

```text
long union(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
```

3. 求文件1减去文件2的差集

```text
long differ(String sourceFilePath1, String sourceFilePath2, String outputFilePath, boolean sort)
```

4. 按字典序排序

```text
long sort(String sourceFilePath, String outputFilePath)
```

5. 过滤

```text
long filter(String sourceFilePath, String outputFilePath)
```

6. 切分

```text
List<String> sharding(String sourceFilePath, int shardingCount)
```

7. 支持自定义文件数据的哈希规则、过滤规则等，需要实现 RowHandler 接口

```text
/*
    数据格式: {md5},{number}

    c77d5d4c907323260c0b702124804e47,12798332
    63d69bbcd8e7962878c8e0f671fa3ff3,1231267
    ...
 */
BigFileProcessor bigFileProcessor = BigFileProcessor.create(
        new BigFileConfig
                .Builder()
                .setBaseDirectory("/Users/caijt/Desktop/bigfile/")
                .build(),
        new RowHandler() {
        
            // 自定义key的哈希值, 根据该哈希值把数据写入对应的分片文件
            @Override
            public int hash(String key) {
                return key.hashCode();
            }

            // 自定义哪个字段为key, key相同的数据在进行集合计算时会通过clash解决冲突
            @Override
            public String getKey(String row) {
                String[] part = row.split(",");
                return part[0];
            }

            // 自定义key冲突时的处理方式
            @Override
            public String clash(String row1, String row2) {
                String[] part1 = row1.split(",");
                String[] part2 = row2.split(",");
                return part1[1] + "," + part2[1];
            }

            // 自定义不合法数据的过滤规则
            @Override
            public boolean accept(String row) {
                return row != null && row.length() > 0 && row.split(",").length == 2;
            }
        });
```