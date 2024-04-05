package com.dfs.loong.server;

import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.task.EditsLogFetcher;
import com.dfs.loong.task.FSImageCheckPoint;
import jdk.nashorn.internal.ir.annotations.Reference;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 负责同步 editsLog 的进程
 */

@Service
public class BuckUpNode implements InitializingBean {

    private volatile Boolean isRunning = true;

    @Reference
    private NameNodeRpc nameNodeRpc;

    @Autowired
    private FSNamesystem fsNamesystem;

    @Override
    public void afterPropertiesSet() {
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(this, nameNodeRpc, fsNamesystem);
        editsLogFetcher.start();

        FSImageCheckPoint fsImageCheckPoint = new FSImageCheckPoint(this, nameNodeRpc, fsNamesystem);
        fsImageCheckPoint.start();
    }

    public Boolean isRunning() {
        return isRunning;
    }

    public void run() {
        while (isRunning) {

            try {
                Thread.sleep(1000L);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
