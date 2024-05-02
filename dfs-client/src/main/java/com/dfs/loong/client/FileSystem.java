package com.dfs.loong.client;

public interface FileSystem {

    /**
     * 创建目录
     */
    void mkdir(String path);

    /**
     * 优雅关闭
     */
    void shutdown();

    /**
     * 上传文件
     * @param file
     * @param fileName
     * @throws Exception
     */
    Boolean upload(byte[] file, String fileName, Long size) throws Exception;
}
