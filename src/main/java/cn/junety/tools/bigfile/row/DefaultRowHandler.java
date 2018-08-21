package cn.junety.tools.bigfile.row;

/**
 * Created by caijt on 2018/8/20
 */
public class DefaultRowHandler implements RowHandler {

    @Override
    public int hash(String key) {
        return key.hashCode();
    }

    @Override
    public String getKey(String row) {
        return row;
    }

    @Override
    public String clash(String row1, String row2) {
        return row1;
    }

    @Override
    public boolean accept(String row) {
        return true;
    }
}
