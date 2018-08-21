package cn.junety.tools.bigfile.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by caijt on 2018/8/17
 */
public class IOUtils {

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final IOException ignored) {
            // ignore
        }
    }

    public static void closeQuietly(Closeable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (Closeable closeable : closeables) {
                closeQuietly(closeable);
            }
        }
    }
}
