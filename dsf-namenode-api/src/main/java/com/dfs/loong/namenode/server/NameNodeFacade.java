package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.EditLog;

import java.util.List;

public interface NameNodeFacade {

    /**
     * 注册 dataNode
     * @param ip
     * @param hostname
     */
    Boolean register(String ip, String hostname);

    /**
     * dataNode 心跳
     * @param ip
     * @param hostname
     * @return
     */
    Boolean heartbeat(String ip, String hostname);

    /**
     * 创建文件夹
     * @param path
     */
    void mkdir(String path);

    /**
     * 优雅关闭接口
     */
    void shutdownClose();


    List<EditLog> fetchEditsLog(long syncedTxid);

    void updateCheckpointTxid(Long maxTxId);
}
