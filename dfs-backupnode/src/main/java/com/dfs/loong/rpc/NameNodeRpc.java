package com.dfs.loong.rpc;

import com.dfs.loong.namenode.server.NameNodeFacade;
import com.dfs.loong.namenode.vo.EditLog;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class NameNodeRpc {

    @Reference
    private NameNodeFacade nameNodeFacade;

    private Boolean isNamenodeRunning = true;

    public List<EditLog> fetchEditsLog(long syncedTxid) {
        return nameNodeFacade.fetchEditsLog(syncedTxid);
    }

    public void updateCheckpointTxid(Long maxTxId) {
        nameNodeFacade.updateCheckpointTxid(maxTxId);
    }

    public Boolean isNamenodeRunning() {
        return isNamenodeRunning;
    }
    public void setIsNamenodeRunning(Boolean isNamenodeRunning) {
        this.isNamenodeRunning = isNamenodeRunning;
    }
}
