package com.dfs.loong.namenode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dfs.loong.namenode.vo.DataNodeInfo;
import com.dfs.loong.namenode.vo.EditLog;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * NameNode的rpc服务的接口
 * @author zhonghuashishan
 *
 */
@Component
@Service(interfaceClass=NameNodeFacade.class)
@Slf4j
public class NameNodeServiceImpl implements NameNodeFacade {

	public static final Integer BACKUP_NODE_FETCH_SIZE = 10;

	/**
	 * 负责管理元数据的核心组件
	 */
	@Autowired
	private FSNamesystem namesystem;

	/**
	 * 负责管理集群中所有的datanode的组件
	 */
	@Autowired
	private DataNodeManager datanodeManager;

	private Boolean isRunning = true;

	/**
	 * 当前缓冲的一小部分editslog
	 */
	private List<EditLog> currentBufferedEditsLog = new ArrayList<>();

	/**
	 * 当前内存里缓冲了哪个磁盘文件的数据
	 */
	private String bufferedFlushedTxid;

	/**
	 * 当前缓存里的editslog最大的一个txid
	 */
	private long currentBufferedMaxTxid = 0L;

	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否创建成功
	 */
	@Override
	public void mkdir(String path){
		if (isRunning) {
			this.namesystem.mkdir(path);
			return;
		}
		log.info("程序正在关闭，不再写入数据。。");
	}

	@Override
	public void shutdownClose() {
		isRunning = false;
		this.namesystem.shutdown();
		this.namesystem.saveCheckpointTxid();
	}

	@Override
	public List<EditLog> fetchEditsLog(long syncedTxid) {
		if(!isRunning) {

			return Lists.newArrayList();
		}
		List<String> flushedTxIds = namesystem.getEditLog().getFlushedTxIds();
		List<EditLog> editLogList = new ArrayList<>();

		// 如果此时 还没有刷出来任何磁盘文件的话，那么此时数据仅仅存在于内存缓冲中
		if (CollectionUtils.isEmpty(flushedTxIds)) {
			fetchFromBufferedEditsLog(syncedTxid, editLogList);
		}
		// 如果此时发现已经有落地磁盘的文件了，这个时候就要扫描所有的磁盘文件的索引范围
		else {
			// 第一种情况，你要拉取的txid是在某个磁盘文件里的
			// 有磁盘文件，而且内存里还缓存了某个磁盘文件的数据了
			if (bufferedFlushedTxid != null) {
				// 如果要拉取的数据就在当前缓存的磁盘文件数据里
				if (existInFlushedFile(syncedTxid, bufferedFlushedTxid)) {
					fetchFromCurrentBuffer(syncedTxid, editLogList);
				}
				// 如果要拉取的数据不在当前缓存的磁盘文件数据里了，那么需要从下一个磁盘文件去拉取
				else {
					String nextFlushedTxid = getNextFlushedTxid(flushedTxIds, bufferedFlushedTxid);
					// 如果可以找到下一个磁盘文件，那么就从下一个磁盘文件里开始读取数据
					if (StringUtils.isNotBlank(nextFlushedTxid)) {
						fetchFromFlushedFile(syncedTxid, nextFlushedTxid, editLogList);
					}
					// 如果没有找到下一个文件，此时就需要从内存里去继续读取
					else {
						fetchFromBufferedEditsLog(syncedTxid, editLogList);
					}
				}
			} else {
				// 遍历所有的磁盘文件的索引范围，0-390，391-782
				Boolean fechedFromFlushedFile = false;

				for(String flushedTxid : flushedTxIds) {
					// 如果要拉取的下一条数据就是在某个磁盘文件里
					if(existInFlushedFile(syncedTxid, flushedTxid)) {
						// 此时可以把这个磁盘文件里以及下一个磁盘文件的的数据都读取出来，放到内存里来缓存
						// 就怕一个磁盘文件的数据不足够10条
						fetchFromFlushedFile(syncedTxid, flushedTxid, editLogList);
						fechedFromFlushedFile = true;
						break;
					}
				}

				// 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
				// 如果没有找到下一个文件，此时就需要从内存里去继续读取
				if(!fechedFromFlushedFile) {
					fetchFromBufferedEditsLog(syncedTxid, editLogList);
				}
			}
		}
		return editLogList;
	}

	@Override
	public void updateCheckpointTxid(Long maxTxId) {
		namesystem.setCheckpointTxid(maxTxId);
	}

	@Override
	public Boolean create(String fileName) {
		// 把文件名的查重和创建文件放在一起来执行
		// 如果说很多个客户端万一同时要发起文件创建，都有一个文件名过来
		// 多线程并发的情况下，文件名的查重和创建都是正确执行的
		// 就必须得在同步的代码块来执行这个功能逻辑
		if(!isRunning) {
			return Boolean.FALSE;
		}

		return namesystem.create(fileName);
	}

	@Override
	public List<DataNodeInfo> allocateDataNodes(String fileName, long fileSize) {
		List<DataNodeInfo> datanodes = datanodeManager.allocateDataNodes(fileSize);
		return datanodes;
	}


	/**
	 * 从已经刷入磁盘的文件里读取editslog，同时缓存这个文件数据到内存
	 * @param flushedTxid
	 */
	private void fetchFromFlushedFile(long syncedTxid, String flushedTxid, List<EditLog> fetchedEditsLog) {
		try {
			String[] flushedTxidSplited = flushedTxid.split("_");
			long startTxid = Long.parseLong(flushedTxidSplited[0]);
			long endTxid = Long.parseLong(flushedTxidSplited[1]);

			String currentEditsLogFile = "/Users/xiongtaolong/Documents/dfs/" + (startTxid) + "_" + endTxid + ".log";

			List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile),
					StandardCharsets.UTF_8);

			currentBufferedEditsLog.clear();
			for(String editsLog : editsLogs) {
				currentBufferedEditsLog.add(JSONObject.parseObject(editsLog, EditLog.class));
			}
			bufferedFlushedTxid = flushedTxid; // 缓存了某个刷入磁盘文件的数据

			fetchFromCurrentBuffer(syncedTxid, fetchedEditsLog);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取下一个磁盘文件对应的 txid 范围
	 * @param flushedTxIds
	 * @param bufferedFlushedTxid
	 * @return
	 */
	private String getNextFlushedTxid(List<String> flushedTxIds, String bufferedFlushedTxid) {
		for(int i = 0; i < flushedTxIds.size(); i++) {
			if(flushedTxIds.get(i).equals(bufferedFlushedTxid)) {
				if(i + 1 < flushedTxIds.size()) {
					return flushedTxIds.get(i + 1);
				}
			}
		}
		return null;
	}

	private boolean existInFlushedFile(long syncedTxid, String flushedTxid) {
		String[] flushedTxidSplited = flushedTxid.split("_");

		long startTxid = Long.valueOf(flushedTxidSplited[0]);
		long endTxid = Long.valueOf(flushedTxidSplited[1]);
		long fetchTxid = syncedTxid + 1;

		if(fetchTxid >= startTxid && fetchTxid <= endTxid) {
			return true;
		}

		return false;
	}

	/**
	 * 就是从内存缓冲的editslog中拉取数据
	 * @param editLogList
	 */
	private void fetchFromBufferedEditsLog(long syncedTxid, List<EditLog> editLogList) {
		long fetchTxid = syncedTxid + 1;
		if(fetchTxid <= currentBufferedMaxTxid) {
			System.out.println("尝试从内存缓冲拉取的时候，发现上一次内存缓存有数据可供拉取......");
			fetchFromCurrentBuffer(syncedTxid, editLogList);
			return;
		}

		currentBufferedEditsLog.clear();

		String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
		if (bufferedEditsLog == null) {
			return;
		}

		for (String editsLog : bufferedEditsLog) {
			currentBufferedEditsLog.add(JSONObject.parseObject(editsLog, EditLog.class));
		}
		bufferedFlushedTxid = null;

		fetchFromCurrentBuffer(syncedTxid, editLogList);
	}

	/**
	 * 从当前已经在内存里缓存的数据中拉取editslog
	 * @param editLogList
	 */
	private void fetchFromCurrentBuffer(long syncedTxid, List<EditLog> editLogList) {
		int fetchCount = 0;
		for (EditLog editLog : currentBufferedEditsLog) {
			if (editLog.getTxid() == syncedTxid + 1) {
				editLogList.add(editLog);
				syncedTxid = editLog.getTxid();
				fetchCount ++;
			}
			if(fetchCount == BACKUP_NODE_FETCH_SIZE) {
				break;
			}
		}
	}

	/**
	 * datanode进行注册
	 * @param ip
	 * @param hostname
	 */
	@Override
	public Boolean register(String ip, String hostname) {
		return datanodeManager.register(ip, hostname);
	}

	/**
	 * datanode进行心跳
	 * @param ip
	 * @param hostname
	 */
	@Override
	public Boolean heartbeat(String ip, String hostname) {
		return datanodeManager.heartbeat(ip, hostname);

	}
}
