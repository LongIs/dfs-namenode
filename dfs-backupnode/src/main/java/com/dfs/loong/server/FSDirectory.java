package com.dfs.loong.server;

import com.alibaba.fastjson.JSON;
import com.dfs.loong.dto.FSImageDTO;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 负责管理内存中的文件目录树的核心组件
 * @author zhonghuashishan
 *
 */
public class FSDirectory {
	
	/**
	 * 内存中的文件目录树
	 */
	private INodeDirectory dirTree;

	private Long maxTxid;

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// 他就是一个父子层级关系的数据结构，文件目录树
	// 创建目录，删除目录，重命名目录，创建文件，删除文件，重命名文件
	// 诸如此类的一些操作，都是在维护内存里的文件目录树，其实本质都是对这个内存的数据结构进行更新

	// 先创建了一个目录层级结果：/usr/warehosue/hive
	// 如果此时来创建另外一个目录：/usr/warehouse/spark

	public FSDirectory() {
		this.dirTree = new INodeDirectory("/");  	// 默认刚开始就是空的节点
	}


	public FSImageDTO getFSImageByJson() {
		try {
			lock.readLock().lock();;
			FSImageDTO dto = new FSImageDTO();
			dto.setMaxTxId(maxTxid);
			dto.setFSImageData(JSON.toJSONString(dirTree));
			return dto;
		} finally {
			lock.readLock().unlock();
		}
	}

	/**
	 * 创建目录
	 * @param path 目录路径
	 */
	public void mkdir(long txid, String path) {
		// path = /usr/warehouse/hive
		// 你应该先判断一下，“/”根目录下有没有一个“usr”目录的存在
		// 如果说有的话，那么再判断一下，“/usr”目录下，有没有一个“/warehouse”目录的存在
		// 如果说没有，那么就得先创建一个“/warehosue”对应的目录，挂在“/usr”目录下
		// 接着再对“/hive”这个目录创建一个节点挂载上去

		// 内存数据结构，更新的时候必须得加锁的
		try {
			lock.writeLock().lock();

			this.maxTxid = txid;

			String[] pathes = path.split("/");
			INodeDirectory parent = dirTree;
			
			for(String splitedPath : pathes) {	// ["","usr","warehosue","spark"]
				if(splitedPath.trim().equals("")) {
					continue;
				}
				
				INodeDirectory dir = findDirectory(parent, splitedPath);	// parent="/usr"
				if(dir != null) {
					parent = dir;
					continue;
				}
				
				INodeDirectory child = new INodeDirectory(splitedPath); 	// "/usr"
				parent.addChild(child);
				parent = child;
			}
		} finally {
			lock.writeLock().unlock();
		}
	}
	
	/**
	 *  查找子目录
	 * @param dir
	 * @param path
	 * @return
	 */
	private INodeDirectory findDirectory(INodeDirectory dir, String path) {
		if(dir.getChildren().size() == 0) {
			return null;
		}
		
		for(INode child : dir.getChildren()) {
			if(child instanceof INodeDirectory) {
				INodeDirectory childDir = (INodeDirectory) child;
				
				if((childDir.getPath().equals(path))) {
					return childDir;
				}
			}
		}
		
		return null;
	}

	public Long getMaxTxid() {
		return maxTxid;
	}

	public void setMaxTxid(Long txid) {
		this.maxTxid = txid;
	}

	public INodeDirectory getDirTree() {
		return dirTree;
	}

	public void setDirTree(INodeDirectory dirTree) {
		this.dirTree = dirTree;
	}

	public Boolean create(String filename) {
		// /image/product/img001.jpg
		// 其实完全可以把前面的路径部分截取出来，去找对应的目录
		synchronized(dirTree) {
			String[] splitedFilename = filename.split("/");
			String realFilename = splitedFilename[splitedFilename.length - 1];

			INodeDirectory parent = dirTree;

			for(int i = 0; i < splitedFilename.length - 1; i++) {
				if(i == 0) {
					continue;
				}

				INodeDirectory dir = findDirectory(parent, splitedFilename[i]);

				if(dir != null) {
					parent = dir;
					continue;
				}

				INodeDirectory child = new INodeDirectory(splitedFilename[i]);
				parent.addChild(child);
				parent = child;
			}

			// 此时就已经获取到了文件的上一级目录
			// 可以查找一下当前这个目录下面是否有对应的文件了
			if(existFile(parent, realFilename)) {
				return false;
			}

			// 真正的在目录里创建一个文件出来
			INodeDirectory file = new INodeDirectory(realFilename);
			parent.addChild(file);
			return true;
		}
	}

	private boolean existFile(INodeDirectory dir, String filename) {
		if(dir.getChildren() != null && dir.getChildren().size() > 0) {
			for(INodeDirectory child : dir.getChildren()) {
				if(child.getPath().equals(filename)) {
					return true;
				}
			}
		}
		return false;
	}


	/**
	 * 代表的是文件目录树中的一个节点
	 * @author zhonghuashishan
	 *
	 */
	private interface INode {

	}
	
	/**
	 * 代表文件目录树中的一个目录
	 * @author zhonghuashishan
	 *
	 */
	public static class INodeDirectory implements INode {
		
		private String path;
		private List<INodeDirectory> children;
		
		public INodeDirectory(String path) {
			this.path = path;
			this.children = new LinkedList<>();
		}



		public void addChild(INodeDirectory inode) {
			this.children.add(inode);
		}
		
		public String getPath() {
			return path;
		}
		public void setPath(String path) {
			this.path = path;
		}
		public List<INodeDirectory> getChildren() {
			return children;
		}
		public void setChildren(List<INodeDirectory> children) {
			this.children = children;
		}
		
	}
	
	/**
	 * 代表文件目录树中的一个文件
	 * @author zhonghuashishan
	 *
	 */
	public static class INodeFile implements INode {
		
		private String name;

		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		
	}
	
}
