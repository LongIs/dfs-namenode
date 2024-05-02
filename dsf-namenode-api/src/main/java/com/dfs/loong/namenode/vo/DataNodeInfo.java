package com.dfs.loong.namenode.vo;

/**
 * 用来描述datanode的信息
 * @author zhonghuashishan
 *
 */
public class DataNodeInfo implements Comparable<DataNodeInfo> {

	private String ip;
	private String hostname;
	private long latestHeartbeatTime = System.currentTimeMillis();

	/**
	 * 已经存储数据的大小
	 */
	private long storedDataSize;

	public DataNodeInfo(String ip, String hostname) {
		this.ip = ip;
		this.hostname = hostname;
	}
	
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public long getLatestHeartbeatTime() {
		return latestHeartbeatTime;
	}
	public void setLatestHeartbeatTime(long latestHeartbeatTime) {
		this.latestHeartbeatTime = latestHeartbeatTime;
	}

	@Override
	public int compareTo(DataNodeInfo o) {
		if(this.storedDataSize - o.getStoredDataSize() > 0) {
			return 1;
		} else if(this.storedDataSize - o.getStoredDataSize() < 0) {
			return -1;
		} else {
			return 0;
		}
	}

	public void addStoredDataSize(long storedDataSize) {
		this.storedDataSize += storedDataSize;
	}

	public long getStoredDataSize() {
		return storedDataSize;
	}

	public void setStoredDataSize(long storedDataSize) {
		this.storedDataSize = storedDataSize;
	}
}
