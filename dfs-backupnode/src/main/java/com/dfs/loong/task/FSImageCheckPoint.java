package com.dfs.loong.task;

import com.dfs.loong.dto.FSImageDTO;
import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.server.BuckUpNode;
import com.dfs.loong.server.FSNamesystem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FSImageCheckPoint extends Thread{


    BuckUpNode buckUpNode;
    NameNodeRpc nameNodeRpc;
    FSNamesystem fsNamesystem;

    private String lastEditsLogFilePath = "";

    public FSImageCheckPoint(BuckUpNode buckUpNode, NameNodeRpc nameNodeRpc, FSNamesystem fsNamesystem) {
        this.buckUpNode = buckUpNode;
        this.nameNodeRpc = nameNodeRpc;
        this.fsNamesystem = fsNamesystem;
    }


    @Override
    public void run() {

        while (buckUpNode.isRunning()) {
            try {
                Thread.sleep(1000);

                FSImageDTO fsImageByJson = fsNamesystem.getFSImageByJson();
                deleteLastFSImageFile();
                doCheckPoint(fsImageByJson);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteLastFSImageFile() {
        File file = new File(lastEditsLogFilePath);
        if (file.exists()) {
            file.delete();
        }
    }

    public void doCheckPoint(FSImageDTO fsImageByJson) {
        ByteBuffer dataBuffer = ByteBuffer.wrap(fsImageByJson.getFSImageData().getBytes());
        String editsLogFilePath = "/Users/xiongtaolong/Documents/dfs/" + fsImageByJson.getMaxTxId() + ".mata";

        lastEditsLogFilePath = editsLogFilePath;

        RandomAccessFile file = null;
        FileOutputStream out = null;
        FileChannel editsLogFileChannel = null;

        try {
            file = new RandomAccessFile(editsLogFilePath, "rw"); // 读写模式，数据写入缓冲区中
            out = new FileOutputStream(file.getFD());
            editsLogFileChannel = out.getChannel();

            editsLogFileChannel.write(dataBuffer);
            editsLogFileChannel.force(false);  // 强制把数据刷入磁盘上

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(out != null) {
                    out.close();
                }
                if(file != null) {
                    file.close();
                }
                if(editsLogFileChannel != null) {
                    editsLogFileChannel.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
