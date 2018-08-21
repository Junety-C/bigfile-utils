package cn.junety.tools.bigfile.row;

/**
 * Created by caijt on 2018/8/17
 */
public interface RowHandler {

    /**
     * 计算key的哈希值, 可以自定义哈希方法
     *
     * @param key key
     * @return 哈希值
     */
    int hash(String key);

    /**
     * 获取key的值, 可以进行自定义key值
     * 例如每行数据是一个json结构, 可以自定义哪个字段作为key
     *
     * @param row 数据行
     * @return key
     */
    String getKey(String row);

    /**
     * 当进行大文件计算, 遇到key值相同时, 需要进行冲突处理
     * 可以自定义需要保留哪一行数据, 或者是进行数据合并操作等等
     *
     * @param row1 数据行1
     * @param row2 数据行2
     * @return 处理后的数据行
     */
    String clash(String row1, String row2);

    /**
     * 过滤掉不合法的数据
     *
     * @param row 数据行
     * @return true:合法 / false:不合法
     */
    boolean accept(String row);
}
