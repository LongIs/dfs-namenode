package com.dfs.loong.namenode.server;

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

    void mkdir(String path);

}
