package com.dfs.loong.namenode.server;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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

	public Boolean create(String fileName) {
		if (!directory.create(fileName)) {
			return false;
		}
		editLog.logEdit("{'OP':'CREATE','PATH':'" + fileName + "'}");
		return true;
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

	/**
	 * 恢复元数据
	 */
	public void recoverNamespace() {
		try {
			loadFSImage();
			loadCheckpointTxid();
			loadEditLog();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void loadEditLog() throws Exception {
		File dir = new File("/Users/xiongtaolong/Documents/dfs");

		List<File> files = new ArrayList<File>();
		for(File file : dir.listFiles()) {
			files.add(file);
		}

		files.removeIf(file -> !file.getName().contains("edits"));

		Collections.sort(files, (o1, o2) -> {
			Integer o1StartTxid = Integer.valueOf(o1.getName().split("-")[1]);
			Integer o2StartTxid = Integer.valueOf(o2.getName().split("-")[1]);
			return o1StartTxid - o2StartTxid;
		});

		if(files.size() == 0) {
			System.out.println("当前没有任何editlog文件，不进行恢复......");
			return;
		}

		for(File file : files) {
			if(file.getName().contains("edits")) {
				System.out.println("准备恢复editlog文件中的数据：" + file.getName());

				String[] splitedName = file.getName().split("-");
				long startTxid = Long.parseLong(splitedName[1]);
				long endTxid = Long.parseLong(splitedName[2].split("[.]")[0]);

				// 如果是checkpointTxid之后的那些editlog都要加载出来
				if(endTxid > checkpointTxid) {
					String currentEditsLogFile = "/Users/xiongtaolong/Documents/dfs/edits-"
							+ startTxid + "-" + endTxid + ".log";

					List<String> editsLogs = Files.readAllLines(Paths.get(currentEditsLogFile),
							StandardCharsets.UTF_8);

					for(String editLogJson : editsLogs) {
						JSONObject editLog = JSONObject.parseObject(editLogJson);
						long txid = editLog.getLongValue("txid");

						if(txid > checkpointTxid) {
							System.out.println("准备回放editlog：" + editLogJson);

							// 回放到内存里去
							String op = editLog.getString("OP");

							if(op.equals("MKDIR")) {
								String path = editLog.getString("PATH");
								try {
									directory.mkdir(path);
								} catch (Exception e) {
									e.printStackTrace();
								}
							} else if(op.equals("CREATE")) {
								String filename = editLog.getString("PATH");
								try {
									directory.create(filename);
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			}
		}
	}

	private void loadCheckpointTxid() throws IOException {

		FileInputStream in = null;
		FileChannel channel = null;
		try {
			String path = "/Users/xiongtaolong/Documents/dfs/checkpoint-txid.meta";

			File file = new File(path);
			if(!file.exists()) {
				System.out.println("checkpoint txid文件不存在，不进行恢复.......");
				return;
			}

			in = new FileInputStream(path);
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024); // 这个参数是可以动态调节的
			// 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
			// 每次重启就分配对应空间的大小就可以了
			int count = channel.read(buffer);

			buffer.flip();
			long checkpointTxid = Long.valueOf(new String(buffer.array(), 0, count));
			System.out.println("恢复checkpoint txid：" + checkpointTxid);

			this.checkpointTxid = checkpointTxid;
		} finally {
			if(in != null) {
				in.close();
			}
			if(channel != null) {
				channel.close();
			}
		}
	}

	/**
	 * 加载fsimage文件到内存里来进行恢复
	 */
	private void loadFSImage() throws Exception {
		FileInputStream in = null;
		FileChannel channel = null;
		try {
			in = new FileInputStream("/Users/xiongtaolong/Documents/dfs/fsimage.meta");
			channel = in.getChannel();

			ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024); // 这个参数是可以动态调节的
			// 每次你接受到一个fsimage文件的时候记录一下他的大小，持久化到磁盘上去
			// 每次重启就分配对应空间的大小就可以了
			int count = channel.read(buffer);

			buffer.flip();
			String fsimageJson = new String(buffer.array(), 0, count);
			FSDirectory.INodeDirectory dirTree = JSONObject.parseObject(fsimageJson, FSDirectory.INodeDirectory.class);
			directory.setDirTree(dirTree);
		} finally {
			if(in != null) {
				in.close();
			}
			if(channel != null) {
				channel.close();
			}
		}
	}

	/**
	 * 将checkpoint txid保存到磁盘上去
	 */
	public void saveCheckpointTxid() {
		String path = "/Users/xiongtaolong/Documents/dfs/checkpoint-txid.meta";

		RandomAccessFile raf = null;
		FileOutputStream out = null;
		FileChannel channel = null;

		try {
			File file = new File(path);
			if(file.exists()) {
				file.delete();
			}

			ByteBuffer buffer = ByteBuffer.wrap(String.valueOf(checkpointTxid).getBytes());

			raf = new RandomAccessFile(path, "rw");
			out = new FileOutputStream(raf.getFD());
			channel = out.getChannel();

			channel.write(buffer);
			channel.force(false);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(out != null) {
					out.close();
				}
				if(raf != null) {
					raf.close();
				}
				if(channel != null) {
					channel.close();
				}
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}
	}
}
