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


    public List<EditLog> fetchEditsLog() {
        return nameNodeFacade.fetchEditsLog();
    }

    public void updateCheckpointTxid(Long maxTxId) {
        nameNodeFacade.updateCheckpointTxid(maxTxId);
    }
}
