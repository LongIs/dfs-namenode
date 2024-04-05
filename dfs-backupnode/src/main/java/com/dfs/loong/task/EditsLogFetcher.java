package com.dfs.loong.task;

import com.alibaba.fastjson.JSON;
import com.dfs.loong.namenode.vo.EditLog;
import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.server.BuckUpNode;
import com.dfs.loong.server.FSNamesystem;

import java.util.List;

public class EditsLogFetcher extends Thread {


    private BuckUpNode buckUpNode;
    private NameNodeRpc nameNodeRpc;
    private FSNamesystem fsNamesystem;


    public EditsLogFetcher(BuckUpNode buckUpNode, NameNodeRpc nameNodeRpc, FSNamesystem fsNamesystem) {
        this.buckUpNode = buckUpNode;
        this.nameNodeRpc = nameNodeRpc;
        this.fsNamesystem = fsNamesystem;
    }

    @Override
    public void run() {
        while (buckUpNode.isRunning()) {
            List<EditLog> editLogs = nameNodeRpc.fetchEditsLog();
            for (EditLog editLog : editLogs) {
                fsNamesystem.mkdir(editLog.getTxid(), JSON.toJSONString(editLog));
            }
        }
    }
}
