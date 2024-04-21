package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.EditLog;
import org.springframework.stereotype.Service;

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
}
