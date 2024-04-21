package com.dfs.loong.task;

import com.dfs.loong.dto.FSImageDTO;
import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.server.BuckUpNode;
import com.dfs.loong.server.FSImageUploader;
import com.dfs.loong.server.FSNamesystem;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FSImageCheckPoint extends Thread{


    BuckUpNode buckUpNode;
    NameNodeRpc nameNodeRpc;
    FSNamesystem fsNamesystem;

    private String lastEditsLogFilePath;

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

                doCheckpoint();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 将fsiamge持久化到磁盘上去
     */
    private void doCheckpoint() {
        FSImageDTO fsImageDTO = fsNamesystem.getFSImageByJson();
        removeLastFSImageFile();
        writeFSImageFile(fsImageDTO);
        uploadFSImageFile(fsImageDTO);
    }

    /**
     * 删除上一个fsimage磁盘文件
     */
    private void removeLastFSImageFile() {
        if (StringUtils.isNotBlank(lastEditsLogFilePath)) {
            File file = new File(lastEditsLogFilePath);
            if(file.exists()) {
                file.delete();
            }
        }
    }

    /**
     * 写入最新的fsimage文件
     */
    private void writeFSImageFile(FSImageDTO fsImageDTO) {
        ByteBuffer dataBuffer = ByteBuffer.wrap(fsImageDTO.getFSImageData().getBytes());
        String editsLogFilePath = "/Users/xiongtaolong/Documents/dfs/" + fsImageDTO.getMaxTxId() + ".mata";

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


    /**
     * 上传fsimage文件
     */
    private void uploadFSImageFile(FSImageDTO fsImageDTO) {
        FSImageUploader fsimageUploader = new FSImageUploader(fsImageDTO);
        fsimageUploader.start();
    }
}
