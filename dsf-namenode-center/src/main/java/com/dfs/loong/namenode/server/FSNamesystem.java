package com.dfs.loong.namenode.server;

import com.alibaba.fastjson.JSONObject;
import com.dfs.loong.namenode.vo.EditLog;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
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
	/**
	 * 负责管理edits log写入磁盘的组件
	 */
	private FSEditlog editLog;

	/**
	 * 最近一次checkpoint更新到的txid
	 */
	private long checkpointTxid;

	public FSNamesystem() {
		this.directory = new FSDirectory();
		this.editLog = new FSEditlog();
	}
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否成功
	 */
	public Boolean mkdir(String path) {
		this.directory.mkdir(path);
		this.editLog.logEdit("{'OP':'MKDIR','PATH':'" + path + "'}");
		return true;
	}

	public void shutdown() {
		editLog.flush();
	}

	public FSEditlog getEditLog() {
		return editLog;
	}

	public void setCheckpointTxid(Long maxTxId) {
		System.out.println("接收到checkpoint txid：" + maxTxId);
		this.checkpointTxid = maxTxId;
	}

	public long getCheckpointTxid() {
		return checkpointTxid;
	}

	/**
	 * 恢复元数据
	 */
	public void recoverNamespace() {
		try {
			loadFSImage();
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
			in = new FileInputStream("/Users/xiongtaolong/Documents/dfs/fsimage.meta");
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 这个参数是可以动态调节的
			// 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
			// 每次重启就分配对应空间的大小就可以了
			int count = channel.read(buffer);

			buffer.flip();
			String fsimageJson = new String(buffer.array(), 0, count);
			FSDirectory.INodeDirectory dirTree = JSONObject.parseObject(fsimageJson, FSDirectory.INodeDirectory.class);
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
	 * 将checkpoint txid保存到磁盘上去
	 */
	public void saveCheckpointTxid() {
		String path = "/Users/xiongtaolong/Documents/dfs/checkpoint-txid.meta";

		RandomAccessFile raf = null;
		FileOutputStream out = null;
		FileChannel channel = null;

		try {
			File file = new File(path);
			if(file.exists()) {
				file.delete();
			}

			ByteBuffer buffer = ByteBuffer.wrap(String.valueOf(checkpointTxid).getBytes());

			raf = new RandomAccessFile(path, "rw");
			out = new FileOutputStream(raf.getFD());
			channel = out.getChannel();

			channel.write(buffer);
			channel.force(false);
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
}
