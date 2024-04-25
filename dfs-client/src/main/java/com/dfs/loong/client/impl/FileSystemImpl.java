package com.dfs.loong.client.impl;

import com.dfs.loong.client.FileSystem;
import com.dfs.loong.namenode.server.NameNodeFacade;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.stereotype.Service;

@Service
public class FileSystemImpl implements FileSystem {

    @Reference
    private NameNodeFacade nameNodeFacade;

    @Override
    public void mkdir(String path) {
        nameNodeFacade.mkdir(path);
    }

    @Override
    public void shutdown() {
        nameNodeFacade.shutdownClose();
    }

    @Override
    public void upload(byte[] file, String fileName) throws Exception {
        // 必须先用filename发送一个RPC接口调用到master节点
        // 去尝试在文件目录树里创建一个文件
        // 此时还需要进行查重，如果这个文件已经存在，就不让你上传了

        // 就是找master节点去要多个数据节点的地址
        // 就是你要考虑自己上传几个副本，找对应副本数量的数据节点的地址
        // 尽可能在分配数据节点的时候，保证每个数据节点放的数据量是比较均衡的
        // 保证集群里各个机器上放的数据比较均衡

        // 依次把文件的副本上传到各个数据节点上去
        // 还要考虑到，如果上传的过程中，某个数据节点他上传失败
        // 此时你需要有一个容错机制的考量
    }
}
