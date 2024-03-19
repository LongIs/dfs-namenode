package com.dfs.loong.namenode.server;

import org.apache.dubbo.config.annotation.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * NameNode的rpc服务的接口
 * @author zhonghuashishan
 *
 */
@Component
@Service(interfaceClass=NameNodeFacade.class)
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
	
	/**
	 * 创建目录
	 * @param path 目录路径
	 * @return 是否创建成功
	 */
	@Override
	public void mkdir(String path){
		this.namesystem.mkdir(path);
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
