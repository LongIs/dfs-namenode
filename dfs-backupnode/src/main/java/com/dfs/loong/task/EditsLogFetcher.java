package com.dfs.loong.task;

import com.alibaba.fastjson.JSON;
import com.dfs.loong.namenode.vo.EditLog;
import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.server.BuckUpNode;
import com.dfs.loong.server.FSNamesystem;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class EditsLogFetcher extends Thread {

    public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

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
            try {

                if(!fsNamesystem.isFinishedRecover()) {
                    log.info("当前还没完成元数据恢复，不进行editlog同步......");
                    Thread.sleep(1000);
                    continue;
                }

                long syncedTxid = fsNamesystem.getSyncedTxid();
                List<EditLog> editLogs = nameNodeRpc.fetchEditsLog(syncedTxid);

                if(editLogs.size() == 0) {
                    log.info("没有拉取到任何一条editslog，等待1秒后继续尝试拉取");
                    Thread.sleep(1000);
                    continue;
                }

                if(editLogs.size() < BACKUP_NODE_FETCH_SIZE) {
                    Thread.sleep(1000);
                    log.info("拉取到的edits log不足10条数据，等待1秒后再次继续去拉取");
                }

                for (EditLog editLog : editLogs) {
                    fsNamesystem.mkdir(editLog.getTxid(), JSON.toJSONString(editLog));
                    String op = editLog.get("OP");

                    if(op.equals("MKDIR")) {
                        String path = editLog.get("PATH");
                        try {
                            fsNamesystem.mkdir(Long.parseLong(editLog.get("txid")), path);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } else if(op.equals("CREATE")) {
                        String filename = editLog.get("PATH");
                        try {
                            fsNamesystem.create(filename);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }


                fsNamesystem.setFinishedRecover(true);
            } catch (Exception e) {
                fsNamesystem.setFinishedRecover(true);
            }


        }
    }
}
