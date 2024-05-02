package com.dfs.loong.datanode.server;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * DataNode启动类
 * @author zhonghuashishan
 *
 */
@Service
public class DataNode implements InitializingBean {

	@Autowired
	private NameNodeOfferService nameNodeOfferService; // 负责跟一组NameNode通信的组件

	@Autowired
	private DataNodeNIOServer dataNodeNIOServer;

	@Override
	public void afterPropertiesSet() {
		initialize();
		run();
	}

	/**
	 * 是否还在运行
	 */
	private volatile Boolean shouldRun;
	
	/**
	 * 初始化DataNode
	 */
	private void initialize() {
		this.shouldRun = true;
		this.nameNodeOfferService.start();
		this.dataNodeNIOServer.start();
	}
	
	/**
	 * 运行DataNode
	 */
	private void run() {
		try {
			while(shouldRun) {
				Thread.sleep(1000);  
			}   
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
