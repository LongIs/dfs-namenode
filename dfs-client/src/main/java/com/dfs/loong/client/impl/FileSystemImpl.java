package com.dfs.loong.client.impl;

import com.dfs.loong.client.FileSystem;
import com.dfs.loong.namenode.server.NameNodeFacade;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.stereotype.Service;

@Service
public class FileSystemImpl implements FileSystem {

    @Reference(timeout = 1000000)
    private NameNodeFacade nameNodeFacade;

    @Override
    public void mkdir(String path) {
        nameNodeFacade.mkdir(path);
    }
}
