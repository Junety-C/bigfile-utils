package cn.junety.tools.bigfile.utils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

/**
 * Created by caijt on 2018/8/17
 */
public class FileUtils {

    public static final String FILE_SEPARATOR = File.separator;

    //---------------------------------------- 通用 ----------------------------------------

    /**
     * 获取当前文件/目录的名称
     *
     * @param path 文件/目录路径
     */
    public static String getName(String path) {
        return getName(Paths.get(path));
    }

    /**
     * 获取当前文件/目录的名称
     *
     * @param path 文件路径
     */
    public static String getName(Path path) {
        return path.getFileName().toString();
    }

    /**
     * 获取文件的上级目录路径
     *
     * @param filePath 文件路径
     */
    public static String getParent(String filePath) {
        return getParent(Paths.get(filePath));
    }

    /**
     * 获取文件的上级目录路径
     *
     * @param filePath 文件路径
     */
    public static String getParent(Path filePath) {
        return filePath.getParent().toString() + FILE_SEPARATOR;
    }

    /**
     * 判断是否目录, 路径不存在会抛出 NoSuchFileException 异常
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean isDirectory(String path, LinkOption... options) {
        return isDirectory(Paths.get(path), options);
    }

    /**
     * 判断是否目录, 路径不存在会抛出 NoSuchFileException 异常
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean isDirectory(Path path, LinkOption... options) {
        return Files.isDirectory(path, options);
    }

    /**
     * 判断是否文件
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean isFile(String path, LinkOption... options) {
        return isFile(Paths.get(path), options);
    }

    /**
     * 判断是否文件
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean isFile(Path path, LinkOption... options) {
        return Files.isRegularFile(path, options);
    }

    /**
     * 判断文件/目录是否存在
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean exist(String path, LinkOption... options) {
        return exist(Paths.get(path), options);
    }

    /**
     * 判断文件/目录是否存在
     *
     * @param path 路径
     * @param options 附加参数
     */
    public static boolean exist(Path path, LinkOption... options) {
        return Files.exists(path, options);
    }

    /**
     * 获取文件大小
     *
     * @param filePath 文件路径
     */
    public static long getFileLength(String filePath) {
        return Paths.get(filePath).toFile().length();
    }

    /**
     * 获取文件大小
     *
     * @param filePath 文件路径
     */
    public static long getFileLength(Path filePath) {
        return filePath.toFile().length();
    }

    /**
     * 检查并添加目录路径的尾部分隔符
     *
     * @param directory 目录路径
     */
    public static String checkDirectorySuffixSeparator(String directory) {
        return directory.endsWith(FILE_SEPARATOR) ? directory : directory + FILE_SEPARATOR;
    }

    //---------------------------------------- 创建文件/目录 ----------------------------------------

    /**
     * 创建一个空文件, 父目录不存在则创建, 文件存在会抛出 FileAlreadyExistsException 异常
     *
     * @param filePath 文件路径
     */
    public static void createFile(String filePath) throws IOException {
        createFile(Paths.get(filePath));
    }

    /**
     * 创建一个空文件, 父目录不存在则创建, 文件存在会抛出 FileAlreadyExistsException 异常
     *
     * @param filePath 文件路径
     */
    public static void createFile(Path filePath) throws IOException {
        String parent;
        if (!exist(parent = getParent(filePath))) {
            createDirectories(parent);
        }
        Files.createFile(filePath);
    }

    /**
     * 递归创建目录, 目录存在则忽略
     *
     * @param directory 目录路径
     */
    public static void createDirectories(String directory) throws IOException {
        createDirectories(Paths.get(directory));
    }

    /**
     * 递归创建目录, 目录存在则忽略
     *
     * @param directory 目录路径
     */
    public static void createDirectories(Path directory) throws IOException {
        Files.createDirectories(directory);
    }

    //---------------------------------------- 删除文件/目录 ----------------------------------------

    /**
     * 删除指定文件/目录, 文件/目录不存在会抛出 NoSuchFileException 异常
     *
     * @param filePath 文件/目录路径
     */
    public static void delete(String filePath) throws IOException {
        delete(Paths.get(filePath));
    }

    /**
     * 删除指定文件/目录, 文件/目录不存在会抛出 NoSuchFileException 异常
     *
     * @param filePath 文件/目录路径
     */
    public static void delete(Path filePath) throws IOException {
        if (isDirectory(filePath)) {
            deleteDirectory(filePath);
        } else {
            Files.delete(filePath);
        }
    }

    /**
     * 删除指定目录及其目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    public static void deleteDirectory(Path directory) throws IOException {
        cleanDirectory(directory);
        Files.delete(directory);
    }

    /**
     * 清空目录, 删除该目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    public static void cleanDirectory(String directory) throws IOException {
        cleanDirectory(Paths.get(directory));
    }

    /**
     * 清空目录, 删除该目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    public static void cleanDirectory(Path directory) throws IOException {
        if (!exist(directory)) {
            return;
        }

        if (!isDirectory(directory)) {
            throw new IllegalArgumentException("Not a directory: " + directory);
        }

        List<Path> pathList = Files.list(directory).collect(Collectors.toList());

        IOException exception = null;
        for (Path path : pathList) {
            try {
                if (isDirectory(path)) {
                    deleteDirectory(path);
                } else {
                    Files.delete(path);
                }
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    /**
     * 删除指定文件/目录, 文件/目录不存在则略过
     *
     * @param filePath 文件/目录路径
     */
    public static void deleteIfExists(String filePath) throws IOException {
        deleteIfExists(Paths.get(filePath));
    }

    /**
     * 删除指定文件/目录, 文件/目录不存在则略过
     *
     * @param filePath 文件/目录路径
     */
    public static void deleteIfExists(Path filePath) throws IOException {
        if (exist(filePath)) {
            if (isDirectory(filePath)) {
                deleteDirectoryIfExists(filePath);
            } else {
                Files.deleteIfExists(filePath);
            }
        }
    }

    /**
     * 删除指定目录及其目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    private static void deleteDirectoryIfExists(Path directory) throws IOException {
        cleanDirectoryIfExists(directory);
        Files.deleteIfExists(directory);
    }

    /**
     * 清空目录, 删除该目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    public static void cleanDirectoryIfExists(String directory) throws IOException {
        cleanDirectoryIfExists(Paths.get(directory));
    }

    /**
     * 清空目录, 删除该目录下的所有文件和子目录
     *
     * @param directory 目录路径
     */
    public static void cleanDirectoryIfExists(Path directory) throws IOException {
        if (!exist(directory)) {
            return;
        }

        if (!isDirectory(directory)) {
            throw new IllegalArgumentException("Not a directory: " + directory);
        }

        List<Path> pathList = Files.list(directory).collect(Collectors.toList());

        IOException exception = null;
        for (Path path : pathList) {
            try {
                if (isDirectory(path)) {
                    deleteDirectoryIfExists(path);
                } else {
                    Files.deleteIfExists(path);
                }
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    //---------------------------------------- 读文件 ----------------------------------------

    /**
     * 按行读取文件, 返回一个 List, 默认使用 UTF-8 字符集
     *
     * @param filePath 文件路径
     */
    public static List<String> readLines(String filePath) throws IOException {
        return readLines(Paths.get(filePath));
    }

    /**
     * 按行读取文件, 使用指定的字符集, 返回一个 List
     *
     * @param filePath 文件路径
     * @param cs 字符集
     */
    public static List<String> readLines(String filePath, Charset cs) throws IOException {
        return readLines(Paths.get(filePath), cs);
    }

    /**
     * 按行读取文件, 返回一个 List, 默认使用 UTF-8 字符集
     *
     * @param filePath 文件路径
     */
    public static List<String> readLines(Path filePath) throws IOException {
        return readLines(filePath, StandardCharsets.UTF_8);
    }

    /**
     * 按行读取文件, 使用指定的字符集, 返回一个 List
     *
     * @param filePath 文件路径
     * @param cs 字符集
     */
    public static List<String> readLines(Path filePath, Charset cs) throws IOException {
        BufferedReader br = null;
        try {
            br = openBufferedReader(filePath, cs);
            return readLines(br);
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    /**
     * 按行读取文件, 读取数据流, 返回一个 List
     *
     * @param br 输出流
     */
    private static List<String> readLines(BufferedReader br) throws IOException {
        String line;
        List<String> list = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            list.add(line);
        }
        return list;
    }

    /**
     * 创建 LineIterator
     *
     * @param filePath 文件路径
     */
    public static LineIterator newLineIterator(String filePath) throws IOException {
        return newLineIterator(Paths.get(filePath));
    }

    /**
     * 创建 LineIterator
     *
     * @param filePath 文件路径
     */
    public static LineIterator newLineIterator(Path filePath) throws IOException {
        return new LineIterator(openBufferedReader(filePath));
    }

    /**
     * 打开 BufferedReader
     *
     * @param filePath 文件路径
     */
    private static BufferedReader openBufferedReader(Path filePath) throws IOException {
        return openBufferedReader(filePath, StandardCharsets.UTF_8);
    }

    /**
     * 打开 BufferedReader
     *
     * @param filePath 文件路径
     */
    private static BufferedReader openBufferedReader(Path filePath, Charset cs) throws IOException {
        return new BufferedReader(new InputStreamReader(openInputStream(filePath), cs));
    }

    /**
     * 打开 FileInputStream
     *
     * @param filePath 文件路径
     */
    private static FileInputStream openInputStream(Path filePath) throws IOException {
        File file = filePath.toFile();
        if (!file.exists()) {
            throw new FileNotFoundException("File '" + filePath.toAbsolutePath().toString() + "' does not exist");
        } else {
            if (file.isDirectory()) {
                throw new IOException("File '" + file + "' exists but is a directory");
            }
            if (!file.canRead()) {
                throw new IOException("File '" + file + "' cannot be read");
            }
        }
        return new FileInputStream(filePath.toFile());
    }

    //---------------------------------------- 写文件 ----------------------------------------

    /**
     * 覆盖数据到文件, 文件不存在则新建
     *
     * @param filePath 文件路径
     * @param lines 数据集合
     */
    public static void writeLines(String filePath, Collection<String> lines) throws IOException {
        writeLines(Paths.get(filePath), lines);
    }

    /**
     * 覆盖数据到文件, 文件不存在则新建
     *
     * @param filePath 文件路径
     * @param lines 数据集合
     */
    public static void writeLines(Path filePath, Collection<String> lines) throws IOException {
        writeLines(filePath, lines, false);
    }

    /**
     * 写数据到文件
     *
     * @param filePath 文件路径
     * @param lines 数据集合
     * @param append 是否以追加的形式写入
     */
    public static void writeLines(String filePath, Collection<String> lines, boolean append)
            throws IOException {
        writeLines(Paths.get(filePath), lines, append);
    }

    /**
     * 写数据到文件
     *
     * @param filePath 文件路径
     * @param lines 数据集合
     * @param append 是否以追加的形式写入
     */
    public static void writeLines(Path filePath, Collection<String> lines, boolean append)
            throws IOException {
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = openBufferedWriter(filePath, append);
            writeLines(bufferedWriter, lines);
        } finally {
            IOUtils.closeQuietly(bufferedWriter);
        }
    }

    /**
     * 写数据到文件
     *
     * @param bufferedWriter 输出流
     * @param lines 数据集合
     */
    private static void writeLines(BufferedWriter bufferedWriter, Collection<String> lines)
            throws IOException {
        for (String line : lines) {
            bufferedWriter.write(line);
            bufferedWriter.newLine();
        }
    }

    /**
     * 打开 BufferedWriter, 如果父目录不存在则创建
     *
     * @param filePath 文件路径
     * @param append 是否以追加的形式写入
     */
    private static BufferedWriter openBufferedWriter(Path filePath, boolean append) throws IOException {
        return openBufferedWriter(filePath, StandardCharsets.UTF_8, append);
    }

    /**
     * 打开 BufferedWriter, 如果父目录不存在则创建
     *
     * @param filePath 文件路径
     * @param cs 字符集
     * @param append 是否以追加的形式写入
     */
    private static BufferedWriter openBufferedWriter(Path filePath,  Charset cs, boolean append)
            throws IOException {
        return new BufferedWriter(new OutputStreamWriter(openOutputStream(filePath, append), cs));
    }

    /**
     * 打开 FileOutputStream, 如果父目录不存在则创建
     *
     * @param filePath 文件路径
     * @param append 是否以追加的形式写入
     */
    private static FileOutputStream openOutputStream(Path filePath, boolean append) throws IOException {
        File file = filePath.toFile();
        if (file.exists()) {
            if (file.isDirectory()) {
                throw new IOException("File '" + file + "' exists but is a directory");
            }
            if (!file.canWrite()) {
                throw new IOException("File '" + file + "' cannot be written to");
            }
        } else {
            File parent = file.getParentFile();
            if (parent != null) {
                if (!parent.mkdirs() && !parent.isDirectory()) {
                    throw new IOException("Directory '" + parent + "' could not be created");
                }
            }
        }
        return new FileOutputStream(filePath.toFile(), append);
    }

    //---------------------------------------- 移动文件/目录 ----------------------------------------

    /**
     * 重命名指定的文件/目录, 如果目标文件/目录的父目录不存在, 则进行创建
     *
     * @param srcPath 要重命名的文件/目录的绝对路径
     * @param destPath 目标的文件/目录的绝对路径
     */
    public static void rename(String srcPath, String destPath) throws IOException {
        rename(Paths.get(srcPath), Paths.get(destPath));
    }

    /**
     * 重命名指定的文件/目录, 如果目标文件/目录的父目录不存在, 则进行创建
     *
     * @param srcPath 要重命名的文件/目录的绝对路径
     * @param destPath 目标的文件/目录的绝对路径
     * @param replaceIfExist 如果目标文件存在, 是否进行覆盖
     */
    public static void rename(String srcPath, String destPath, boolean replaceIfExist)
            throws IOException {
        if (srcPath == null) {
            throw new NullPointerException("Source must not be null");
        }
        if (destPath == null) {
            throw new NullPointerException("Destination must not be null");
        }
        if (!exist(srcPath)) {
            throw new FileNotFoundException("Source '" + srcPath + "' does not exist");
        }
        if (exist(destPath)) {
            throw new FileAlreadyExistsException("Destination '" + destPath + "' already exists");
        }
        rename(Paths.get(srcPath), Paths.get(destPath), replaceIfExist);
    }

    /**
     * 重命名指定的文件/目录, 如果目标文件/目录的父目录不存在, 则进行创建
     *
     * @param srcPath 要重命名的文件/目录的绝对路径
     * @param destPath 目标的文件/目录的绝对路径
     * @param replaceIfExist 如果目标文件存在, 是否进行覆盖
     */
    public static void rename(Path srcPath, Path destPath, boolean replaceIfExist) throws IOException {
        if (!exist(getParent(destPath))) {
            createDirectories(getParent(destPath));
        }
        if (replaceIfExist) {
            rename(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);
        } else {
            rename(srcPath, destPath);
        }
    }

    /**
     * 重命名指定的文件/目录
     *
     * @param srcPath 要重命名的文件/目录的绝对路径
     * @param destPath 目标的文件/目录的绝对路径
     */
    public static void rename(Path srcPath, Path destPath, CopyOption... options) throws IOException {
        Files.move(srcPath, destPath, options);
    }

    /**
     * 移动指定目录到目标目录下, 如果目标目录不存在则进行创建
     *
     * @param srcDirectory 要移动的目录的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveDirectoryToDirectory(String srcDirectory, String destDirectory)
            throws IOException {
        if (srcDirectory == null) {
            throw new NullPointerException("Source directory must not be null");
        }
        if (destDirectory == null) {
            throw new NullPointerException("Destination directory must not be null");
        }
        moveDirectoryToDirectory(Paths.get(srcDirectory), Paths.get(destDirectory));
    }

    /**
     * 移动指定目录到目标目录下, 如果目标目录不存在则进行创建
     *
     * @param srcDirectory 要移动的目录的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveDirectoryToDirectory(Path srcDirectory, Path destDirectory) throws IOException {
        if (!exist(srcDirectory)) {
            throw new FileNotFoundException("Source directory '" + srcDirectory + "' does not exist");
        }
        if (!isDirectory(srcDirectory)) {
            throw new IllegalArgumentException("Source '" + srcDirectory + "' is not a directory");
        }
        if (exist(destDirectory)) {
            if (!isDirectory(destDirectory)) {
                throw new IllegalArgumentException("Destination '" + destDirectory + "' is not a directory");
            }
        } else {
            createDirectories(destDirectory);
        }
        Files.move(srcDirectory, Paths.get(destDirectory.toString(), getName(srcDirectory)));
    }

    /**
     * 移动文件到指定目录下, 如果目标目录不存在则进行创建
     *
     * @param srcFilePath 要移动的文件的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveFileToDirectory(String srcFilePath, String destDirectory) throws IOException {
        if (srcFilePath == null) {
            throw new NullPointerException("Source file must not be null");
        }
        if (destDirectory == null) {
            throw new NullPointerException("Destination directory must not be null");
        }
        moveFileToDirectory(Paths.get(srcFilePath), Paths.get(destDirectory));
    }

    /**
     * 移动文件到指定目录下, 如果目标目录不存在则进行创建
     *
     * @param srcFilePath 要移动的文件的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveFileToDirectory(Path srcFilePath, Path destDirectory) throws IOException {
        if (!exist(srcFilePath)) {
            throw new FileNotFoundException("Source file '" + srcFilePath + "' does not exist");
        }
        if (!isFile(srcFilePath)) {
            throw new IllegalArgumentException("Source '" + srcFilePath + "' is not a file");
        }
        if (exist(destDirectory)) {
            if (!isDirectory(destDirectory)) {
                throw new IllegalArgumentException("Destination '" + destDirectory + "' is not a directory");
            }
        } else {
            createDirectories(destDirectory);
        }
        Files.move(srcFilePath, Paths.get(destDirectory.toString(), getName(srcFilePath)));
    }

    /**
     * 移动文件/目录到指定目录下, 如果目标目录不存在则进行创建
     *
     * @param srcPath 要移动的文件/目录的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveToDirectory(String srcPath, String destDirectory) throws IOException {
        if (srcPath == null) {
            throw new NullPointerException("Source must not be null");
        }
        if (destDirectory == null) {
            throw new NullPointerException("Destination directory must not be null");
        }
        moveToDirectory(Paths.get(srcPath), Paths.get(destDirectory));
    }

    /**
     * 移动文件/目录到指定目录下, 如果目标目录不存在则进行创建
     *
     * @param srcPath 要移动的文件/目录的绝对路径
     * @param destDirectory 目标目录的绝对路径
     */
    public static void moveToDirectory(Path srcPath, Path destDirectory) throws IOException {
        if (!exist(srcPath)) {
            throw new FileNotFoundException("Source '" + srcPath + "' does not exist");
        }
        if (isDirectory(srcPath)) {
            moveDirectoryToDirectory(srcPath, destDirectory);
        } else {
            moveFileToDirectory(srcPath, destDirectory);
        }
    }

    //---------------------------------------- 其他 ----------------------------------------

    /**
     * 获取文件行数
     *
     * @param filePath 文件路径
     */
    public static int getLineNumber(String filePath) throws IOException {
        return getLineNumber(Paths.get(filePath));
    }

    /**
     * 获取文件行数
     *
     * @param filePath 文件路径
     */
    public static int getLineNumber(Path filePath) throws IOException {
        try (LineNumberReader lineNumberReader = new LineNumberReader(openBufferedReader(filePath))) {
            lineNumberReader.skip(Long.MAX_VALUE);
            return lineNumberReader.getLineNumber();
        }
    }

    /**
     * 计算文件 crc32 校验和
     *
     * @param filePath 文件路径
     */
    public static long checksumCRC32(String filePath) throws IOException {
        return checksumCRC32(Paths.get(filePath));
    }

    /**
     * 计算文件 crc32 校验和
     *
     * @param filePath 文件路径
     */
    public static long checksumCRC32(Path filePath) throws IOException {
        try (InputStream is = new BufferedInputStream(openInputStream(filePath))) {
            CRC32 crc32 = new CRC32();
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                crc32.update(buffer, 0, length);
            }
            return crc32.getValue();
        }
    }

    /**
     * 计算文件 md5 校验和
     *
     * @param filePath 文件路径
     */
    public static String checksumMD5(String filePath) throws IOException {
        return checksumMD5(Paths.get(filePath));
    }

    /**
     * 计算文件 md5 校验和
     *
     * @param filePath 文件路径
     */
    public static String checksumMD5(Path filePath) throws IOException {
        try (InputStream is = new BufferedInputStream(openInputStream(filePath))) {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) != -1) {
                md5.update(buffer, 0, length);
            }
            return toHexString(md5.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 字节数组转16进制字符串
     *
     * @param bytes 字节数据
     */
    private static String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        if (bytes != null) {
            for (Byte b : bytes) {
                sb.append(String.format("%02X", b.intValue() & 0xFF));
            }
        }
        return sb.toString();
    }
}
