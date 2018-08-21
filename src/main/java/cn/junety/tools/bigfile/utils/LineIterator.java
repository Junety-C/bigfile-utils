package cn.junety.tools.bigfile.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by caijt on 2018/8/17
 */
public class LineIterator implements AutoCloseable {

    private final BufferedReader bufferedReader;
    private String cacheLine;
    private boolean finished;

    public LineIterator(BufferedReader bufferedReader) {
        if (bufferedReader == null) {
            throw new IllegalArgumentException("buffered reader must not be null");
        } else {
            this.bufferedReader = bufferedReader;
            this.cacheLine = null;
            this.finished = false;
        }
    }

    /**
     * 检查还有没有下一行合法的数据, 如果发生 ioe 异常, 会自动 close 数据流, 避免资源泄漏
     */
    public boolean hasNext() {
        if (cacheLine != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {
                while (true) {
                    String line = bufferedReader.readLine();
                    if (line == null) {
                        finished = true;
                        return false;
                    } else if (isValidLine(line)) {
                        cacheLine = line;
                        return true;
                    }
                }
            } catch(IOException ioe) {
                close();
                throw new IllegalStateException(ioe);
            }
        }
    }

    /**
     * 返回下一行合法的数据
     */
    public String nextLine() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }
        String currentLine = cacheLine;
        cacheLine = null;
        return currentLine;
    }

    /**
     * 返回接下来的最多 size 行合法的数据, 当 list.size() != size 时, 可以说明数据流已经读取完毕
     *
     * @param size 最多返回多少行数据
     */
    public List<String> nextLines(int size) {
        List<String> list = new ArrayList<>(size);
        while (hasNext() && list.size() < size) {
            list.add(nextLine());
        }
        return list;
    }

    /**
     * 关闭数据流, 关闭 LineIterator
     */
    @Override
    public void close() {
        IOUtils.closeQuietly(bufferedReader);
        cacheLine = null;
        finished = true;
    }

    /**
     * 默认总是返回 true, 支持自己重写该方法, 从而实现过滤不合法的数据行
     *
     * @param line 数据行
     */
    protected boolean isValidLine(String line) {
        return true;
    }
}
