package com.dfs.loong.namenode.server;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
	 * 当前backupNode节点同步到了哪一条txid了
	 */
	private long syncedTxid = 0L;

	/**
	 * 当前缓冲的一小部分editslog
	 */
	private List<EditLog> currentBufferedEditsLog = new ArrayList<>();

	/**
	 * 当前内存里缓冲了哪个磁盘文件的数据
	 */
	private String bufferedFlushedTxid;

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
	public List<EditLog> fetchEditsLog() {
		if(!isRunning) {

			return Lists.newArrayList();
		}
		List<String> flushedTxIds = namesystem.getEditLog().getFlushedTxIds();
		List<EditLog> editLogList = new ArrayList<>();

		// 如果此时 还没有刷出来任何磁盘文件的话，那么此时数据仅仅存在于内存缓冲中
		if (CollectionUtils.isEmpty(flushedTxIds)) {
			fetchFromBufferedEditsLog(editLogList);
		}
		// 如果此时发现已经有落地磁盘的文件了，这个时候就要扫描所有的磁盘文件的索引范围
		else {
			// 第一种情况，你要拉取的txid是在某个磁盘文件里的
			// 有磁盘文件，而且内存里还缓存了某个磁盘文件的数据了
			if (bufferedFlushedTxid != null) {
				// 如果要拉取的数据就在当前缓存的磁盘文件数据里
				if (existInFlushedFile(bufferedFlushedTxid)) {
					fetchFromCurrentBuffer(editLogList);
				}
				// 如果要拉取的数据不在当前缓存的磁盘文件数据里了，那么需要从下一个磁盘文件去拉取
				else {
					String nextFlushedTxid = getNextFlushedTxid(flushedTxIds, bufferedFlushedTxid);
					// 如果可以找到下一个磁盘文件，那么就从下一个磁盘文件里开始读取数据
					if (StringUtils.isNotBlank(nextFlushedTxid)) {
						fetchFromFlushedFile(nextFlushedTxid, editLogList);
					}
					// 如果没有找到下一个文件，此时就需要从内存里去继续读取
					else {
						fetchFromBufferedEditsLog(editLogList);
					}
				}
			} else {
				// 遍历所有的磁盘文件的索引范围，0-390，391-782
				Boolean fechedFromFlushedFile = false;

				for(String flushedTxid : flushedTxIds) {
					// 如果要拉取的下一条数据就是在某个磁盘文件里
					if(existInFlushedFile(flushedTxid)) {
						// 此时可以把这个磁盘文件里以及下一个磁盘文件的的数据都读取出来，放到内存里来缓存
						// 就怕一个磁盘文件的数据不足够10条
						fetchFromFlushedFile(flushedTxid, editLogList);
						fechedFromFlushedFile = true;
						break;
					}
				}

				// 第二种情况，你要拉取的txid已经比磁盘文件里的全部都新了，还在内存缓冲里
				// 如果没有找到下一个文件，此时就需要从内存里去继续读取
				if(!fechedFromFlushedFile) {
					fetchFromBufferedEditsLog(editLogList);
				}
			}
		}
		return editLogList;
	}

	@Override
	public void updateCheckpointTxid(Long maxTxId) {
		namesystem.setCheckpointTxid(maxTxId);
	}

	/**
	 * 从已经刷入磁盘的文件里读取editslog，同时缓存这个文件数据到内存
	 * @param flushedTxid
	 */
	private void fetchFromFlushedFile(String flushedTxid, List<EditLog> fetchedEditsLog) {
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

			fetchFromCurrentBuffer(fetchedEditsLog);
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

	private boolean existInFlushedFile(String flushedTxid) {
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
	private void fetchFromBufferedEditsLog(List<EditLog> editLogList) {
		currentBufferedEditsLog.clear();

		String[] bufferedEditsLog = namesystem.getEditLog().getBufferedEditsLog();
		if (bufferedEditsLog == null) {
			return;
		}

		for (String editsLog : bufferedEditsLog) {
			currentBufferedEditsLog.add(JSONObject.parseObject(editsLog, EditLog.class));
		}
		bufferedFlushedTxid = null;

		fetchFromCurrentBuffer(editLogList);
	}

	/**
	 * 从当前已经在内存里缓存的数据中拉取editslog
	 * @param editLogList
	 */
	private void fetchFromCurrentBuffer(List<EditLog> editLogList) {
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
