package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.EditLog;
import lombok.extern.slf4j.Slf4j;
import org.apache.dubbo.config.annotation.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
	}

	@Override
	public List<EditLog> fetchEditsLog() {
		return null;
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
