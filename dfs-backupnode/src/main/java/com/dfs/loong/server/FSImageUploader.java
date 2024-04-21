package com.dfs.loong.server;

import com.dfs.loong.dto.FSImageDTO;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 负责上传fsimage到NameNode线程
 * @author zhonghuashishan
 *
 */
public class FSImageUploader extends Thread {

	private FSImageDTO fsImageDTO;
	
	public FSImageUploader(FSImageDTO fsImageDTO) {
		this.fsImageDTO = fsImageDTO;
	}
	
	@Override
	public void run() {
		//SocketChannel 是Java NIO中用于TCP网络通信的通道。它是一个可以连接到远程服务器的通道，并可以用于读写数据。
		SocketChannel channel = null;  
		Selector selector = null;  
		try {  
			channel = SocketChannel.open();  
			channel.configureBlocking(false);
			// 尝试通过非阻塞的方式，使用 channel 这个 SocketChannel 对象连接到本机（localhost）上的9000端口。
			channel.connect(new InetSocketAddress("localhost", 9000)); 
			
			selector = Selector.open();
			// 将通道（Channel）注册到选择器（Selector）上，并指定感兴趣的操作（interest set）为连接操作（OP_CONNECT）。
			channel.register(selector, SelectionKey.OP_CONNECT); 
			
			boolean uploading = true;
			
			while(uploading){  
				selector.select();   
				
				Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();  
				while(keysIterator.hasNext()){  
					SelectionKey key = keysIterator.next();
					keysIterator.remove();
					   
					if(key.isConnectable()){  
						channel = (SocketChannel) key.channel(); 
						
						if(channel.isConnectionPending()){  
							channel.finishConnect();
							ByteBuffer buffer = ByteBuffer.wrap(fsImageDTO.getFSImageData().getBytes());
							System.out.println("准备上传fsimage文件数据，大小为：" + buffer.capacity());  
							channel.write(buffer);  
						}   
						
						channel.register(selector, SelectionKey.OP_READ);
					}  
					else if(key.isReadable()){  
						ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);           
						channel = (SocketChannel) key.channel();
						int count = channel.read(buffer);  
						
						if(count > 0) {
							System.out.println("上传fsimage文件成功，响应消息为：" + 
									new String(buffer.array(), 0, count));
							channel.close();
							uploading = false;
						}
					}
				}  
			}                            
		} catch (Exception e) {  
			e.printStackTrace();  
		} finally{  
			if(channel != null){  
				try {  
					channel.close();  
				} catch (IOException e) {                        
					e.printStackTrace();  
				}                    
			}  
			   
			if(selector != null){  
				try {  
					selector.close();  
				} catch (IOException e) {  
					e.printStackTrace();  
				}  
			}  
		}  
	}
	
}
