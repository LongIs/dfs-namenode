package com.dfs.loong.datanode.server;


import com.dfs.loong.namenode.server.NameNodeFacade;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 负责跟一组NameNode中的某一个进行通信的线程组件
 * @author zhonghuashishan
 *
 */
@Service
public class NameNodeServiceActor {
	
	private static final String NAMENODE_HOSTNAME = "localhost";
	private static final Integer NAMENODE_PORT = 50070;

	@Reference
	private NameNodeFacade nameNodeFacade;

	@Autowired
	private RegisterThread registerThread;
	@Autowired
	private HeartbeatThread heartbeatThread;

	/*private NameNodeServiceGrpc.NameNodeServiceBlockingStub namenode;
	
	public NameNodeServiceActor() {
		ManagedChannel channel = NettyChannelBuilder
				.forAddress(NAMENODE_HOSTNAME, NAMENODE_PORT)
				.negotiationType(NegotiationType.PLAINTEXT)
				.build();
		this.namenode = NameNodeServiceGrpc.newBlockingStub(channel);
	}*/

	/**
	 * 向自己负责通信的那个NameNode进行注册
	 */
	public void register() throws Exception {
		registerThread.start();
		registerThread.join();
	}
	
	/**
	 * 开启发送心跳的线程
	 */
	public void startHeartbeat() {
		heartbeatThread.start();
	}
	
	/**
	 * 负责注册的线程
	 * @author zhonghuashishan
	 *
	 */
	@Service
	class RegisterThread extends Thread {
		
		@Override
		public void run() {
			try {
				// 发送rpc接口调用请求到NameNode去进行注册
				System.out.println("发送RPC请求到NameNode进行注册.......");  
				
				// 在这里进行注册的时候会提供哪些信息过去呢？
				// 比如说当前这台机器的ip地址、hostname，这两个东西假设是写在配置文件里的
				// 我们写代码的时候，主要是在本地来运行和测试，有一些ip和hostname，就直接在代码里写死了
				// 大家后面自己可以留空做一些完善，你可以加一些配置文件读取的代码
				String ip = "127.0.0.1";
				String hostname = "dfs-data-01";
				// 通过RPC接口发送到NameNode他的注册接口上去
				
				/*RegisterRequest request = RegisterRequest.newBuilder()
						.setIp(ip)
						.setHostname(hostname)
						.build();
				RegisterResponse response = namenode.register(request);
				System.out.println("接收到NameNode返回的注册响应：" + response.getStatus());  */

				nameNodeFacade.register(ip, hostname);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 负责心跳的线程
	 * @author zhonghuashishan
	 *
	 */
	@Service
	class HeartbeatThread extends Thread {
		
		@Override
		public void run() {
			try {
				while(true) {
					System.out.println("发送RPC请求到NameNode进行心跳.......");  
					
					String ip = "127.0.0.1";
					String hostname = "dfs-data-01";
					// 通过RPC接口发送到NameNode他的注册接口上去
					
					/*HeartbeatRequest request = HeartbeatRequest.newBuilder()
							.setIp(ip)
							.setHostname(hostname)
							.build();
					HeartbeatResponse response = namenode.heartbeat(request);
					System.out.println("接收到NameNode返回的心跳响应：" + response.getStatus());  */

					nameNodeFacade.heartbeat(ip, hostname);

					Thread.sleep(30 * 1000); // 每隔30秒发送一次心跳到NameNode上去  
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
