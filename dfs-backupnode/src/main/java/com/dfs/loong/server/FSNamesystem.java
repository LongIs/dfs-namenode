package com.dfs.loong.server;

import com.alibaba.fastjson.JSONObject;
import com.dfs.loong.dto.FSImageDTO;
import com.dfs.loong.server.FSDirectory.INodeDirectory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 负责管理元数据的核心组件
 * @author zhonghuashishan
 *
 */
@Service
public class FSNamesystem {

	/**
	 * 负责管理内存文件目录树的组件
	 */
	private FSDirectory directory;

	private long checkpointTime;
	private long syncedTxid;
	private String checkpointFile = "";
	private volatile boolean finishedRecover = false;

	public FSNamesystem() {
		this.directory = new FSDirectory();
		recoverNamespace();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(long txid, String path) {
		this.directory.mkdir(txid, path);
		return true;
	}

	public FSImageDTO getFSImageByJson() {
		return this.directory.getFSImageByJson();
	}

	/**
	 * 恢复元数据
	 */
	public void recoverNamespace() {
		try {
			loadCheckpointInfo();
			loadFSImage();
			finishedRecover = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 加载fsimage文件到内存里来进行恢复
	 */
	private void loadFSImage() throws Exception {
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			String path = "/Users/xiongtaolong/Documents/dfs/backupnode/fsimage-" + syncedTxid + ".meta";
			File file = new File(path);
			if(!file.exists()) {
				System.out.println("fsimage文件当前不存在，不进行恢复.......");
				return;
			}

			in = new FileInputStream(path);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 这个参数是可以动态调节的
			// 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
			// 每次重启就分配对应空间的大小就可以了
			int count = channel.read(buffer);

			buffer.flip();
			String fsimageJson = new String(buffer.array(), 0, count);
			System.out.println("恢复fsimage文件中的数据：" + fsimageJson);

			INodeDirectory dirTree = JSONObject.parseObject(fsimageJson, INodeDirectory.class);
			System.out.println(dirTree);
			directory.setDirTree(dirTree);
		} finally {
			if(in != null) {
				in.close();
			}
			if(channel != null) {
				channel.close();
			}
		}
	}

	/**
	 * 加载checkpoint txid
	 */
	private void loadCheckpointInfo() throws IOException {
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			String path = "/Users/xiongtaolong/Documents/dfs/backupnode/checkpoint-info.meta";

			File file = new File(path);
			if(!file.exists()) {
				System.out.println("checkpoint info文件不存在，不进行恢复.......");
				return;
			}

			in = new FileInputStream(path);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024); // 这个参数是可以动态调节的
			// 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
			// 每次重启就分配对应空间的大小就可以了
			int count = channel.read(buffer);

			buffer.flip();

			String checkpointInfo = new String(buffer.array(), 0, count);
			long checkpointTime = Long.valueOf(checkpointInfo.split("_")[0]);
			long syncedTxid = Long.valueOf(checkpointInfo.split("_")[1]);
			String fsimageFile = checkpointInfo.split("_")[2];

			System.out.println("恢复checkpoint time：" + checkpointTime + ", synced txid: " + syncedTxid + ", fsimage file: " + fsimageFile);

			this.checkpointTime = checkpointTime;
			this.syncedTxid = syncedTxid;
			this.checkpointFile = fsimageFile;
			directory.setMaxTxid(syncedTxid);
		} finally {
			if(in != null) {
				in.close();
			}
			if(channel != null) {
				channel.close();
			}
		}
	}

	public long getCheckpointTime() {
		return checkpointTime;
	}

	public void setCheckpointTime(long checkpointTime) {
		this.checkpointTime = checkpointTime;
	}

	public String getCheckpointFile() {
		return checkpointFile;
	}

	public void setCheckpointFile(String checkpointFile) {
		this.checkpointFile = checkpointFile;
	}

	public boolean isFinishedRecover() {
		return finishedRecover;
	}

	public void setFinishedRecover(boolean finishedRecover) {
		this.finishedRecover = finishedRecover;
	}

	/**
	 * 获取当前同步到的最大的txid
	 * @return
	 */
	public long getSyncedTxid() {
		return directory.getFSImageByJson().getMaxTxId();
	}
}
