package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.DataNodeInfo;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个组件，就是负责管理集群里的所有的datanode的
 * @author zhonghuashishan
 *
 */
@Service
public class DataNodeManager implements InitializingBean {

	/**
	 * 集群中所有的datanode
	 */
	private Map<String, DataNodeInfo> datanodes = 
			new ConcurrentHashMap<String, DataNodeInfo>();

	@Autowired
	private DataNodeAliveMonitor dataNodeAliveMonitor;

	@Override
	public void afterPropertiesSet() {
		dataNodeAliveMonitor.start();
	}
	
	/**
	 * datanode进行注册
	 * @param ip 
	 * @param hostname
	 */
	public Boolean register(String ip, String hostname) {
		DataNodeInfo datanode = new DataNodeInfo(ip, hostname);
		datanodes.put(ip + "-" + hostname, datanode);  
		System.out.println("DataNode注册：ip=" + ip + ",hostname=" + hostname);  
		return true;
	}
	
	/**
	 * datanode进行心跳
	 * @param ip
	 * @param hostname
	 * @return
	 */
	public Boolean heartbeat(String ip, String hostname) {
		DataNodeInfo datanode = datanodes.get(ip + "-" + hostname);
		datanode.setLatestHeartbeatTime(System.currentTimeMillis());  
		System.out.println("DataNode发送心跳：ip=" + ip + ",hostname=" + hostname);  
		return true;
	}

	public List<DataNodeInfo> allocateDataNodes(long fileSize) {
		synchronized (this) {
			// 取出来所有的datanode，并且按照已经存储的数据大小来排序
			List<DataNodeInfo> datanodeList = new ArrayList<>();
			for(DataNodeInfo datanode : datanodes.values()) {
				datanodeList.add(datanode);
			}

			Collections.sort(datanodeList);

			// 选择存储数据最少的头两个datanode出来
			List<DataNodeInfo> selectedDatanodes = new ArrayList<DataNodeInfo>();
			if(datanodeList.size() >= 2) {
				selectedDatanodes.add(datanodeList.get(0));
				selectedDatanodes.add(datanodeList.get(1));

				// 你可以做成这里是副本数量可以动态配置的，但是我这里的话呢给写死了，就是双副本

				// 默认认为：要上传的文件会被放到那两个datanode上去
				// 此时就应该更新那两个datanode存储数据的大小，加上上传文件的大小
				// 你只有这样做了，后面别人再次要过来上传文件的时候，就可以基于最新的存储情况来进行排序了
				datanodeList.get(0).addStoredDataSize(fileSize);
				datanodeList.get(1).addStoredDataSize(fileSize);
			}
			return selectedDatanodes;
		}
	}

	/**
	 * datanode是否存活的监控线程
	 * @author zhonghuashishan
	 *
	 */
	@Service
	class DataNodeAliveMonitor extends Thread {
		
		@Override
		public void run() {
			try {
				while(true) {
					List<String> toRemoveDatanodes = new ArrayList<String>();
					
					Iterator<DataNodeInfo> datanodesIterator = datanodes.values().iterator();
					DataNodeInfo datanode = null;
					while(datanodesIterator.hasNext()) {
						datanode = datanodesIterator.next();
						if(System.currentTimeMillis() - datanode.getLatestHeartbeatTime() > 90 * 1000) {
							toRemoveDatanodes.add(datanode.getIp() + "-" + datanode.getHostname());
						}
					}
					
					if(!toRemoveDatanodes.isEmpty()) {
						for(String toRemoveDatanode : toRemoveDatanodes) {
							datanodes.remove(toRemoveDatanode);
						}
					}
					
					Thread.sleep(30 * 1000); 
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
