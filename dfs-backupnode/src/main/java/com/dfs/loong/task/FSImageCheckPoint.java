package com.dfs.loong.task;

import com.dfs.loong.dto.FSImageDTO;
import com.dfs.loong.rpc.NameNodeRpc;
import com.dfs.loong.server.BuckUpNode;
import com.dfs.loong.server.FSImageUploader;
import com.dfs.loong.server.FSNamesystem;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

@Slf4j
public class FSImageCheckPoint extends Thread{

    /**
     * checkpoint操作的时间间隔
     */
    public static final Integer CHECKPOINT_INTERVAL = 60 * 1000;

    BuckUpNode buckUpNode;
    NameNodeRpc nameNodeRpc;
    FSNamesystem fsNamesystem;

    private String lastEditsLogFilePath;

    private String lastFsimageFile = "";

    private long checkpointTime = System.currentTimeMillis();

    public FSImageCheckPoint(BuckUpNode buckUpNode, NameNodeRpc nameNodeRpc, FSNamesystem fsNamesystem) {
        this.buckUpNode = buckUpNode;
        this.nameNodeRpc = nameNodeRpc;
        this.fsNamesystem = fsNamesystem;
    }


    @Override
    public void run() {

        while (buckUpNode.isRunning()) {
            try {
                if (!fsNamesystem.isFinishedRecover()) {
                    Thread.sleep(1000L);
                    continue;
                }

                if (lastFsimageFile.equals("")) {
                    this.lastFsimageFile = fsNamesystem.getCheckpointFile();
                }

                long now = System.currentTimeMillis();
                if(now - checkpointTime > CHECKPOINT_INTERVAL) {
                    if (!nameNodeRpc.isNamenodeRunning()) {
                        log.info("nameNode 当前无法访问， 不执行 Checkpoint");
                        continue;
                    }
                }

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
        updateCheckpointTxid(fsImageDTO);
        saveCheckpointInfo(fsImageDTO);
    }

    /**
     * 持久化checkpoint信息
     * @param fsImageDTO
     */
    private void saveCheckpointInfo(FSImageDTO fsImageDTO) {
        String path = "/Users/xiongtaolong/Documents/dfs/backupnode/checkpoint-info.meta";

        RandomAccessFile raf = null;
        FileOutputStream out = null;
        FileChannel channel = null;

        try {
            File file = new File(path);
            if(file.exists()) {
                file.delete();
            }

            long now = System.currentTimeMillis();
            this.checkpointTime = now;
            long checkpointTxid = fsImageDTO.getMaxTxId();
            ByteBuffer buffer = ByteBuffer.wrap(String.valueOf(now + "_" + checkpointTxid + "_" + lastFsimageFile).getBytes());

            raf = new RandomAccessFile(path, "rw");
            out = new FileOutputStream(raf.getFD());
            channel = out.getChannel();

            channel.write(buffer);
            channel.force(false);

            System.out.println("checkpoint信息持久化到磁盘文件......");
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(out != null) {
                    out.close();
                }
                if(raf != null) {
                    raf.close();
                }
                if(channel != null) {
                    channel.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }

    /**
     * 更新checkpoint txid
     * @param fsimage
     */
    private void updateCheckpointTxid(FSImageDTO fsimage) {
        nameNodeRpc.updateCheckpointTxid(fsimage.getMaxTxId());
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
