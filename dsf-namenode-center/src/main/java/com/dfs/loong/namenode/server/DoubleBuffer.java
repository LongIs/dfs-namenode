package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.EditLog;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 内存双缓冲
 * @author zhonghuashishan
 *
 */
public class DoubleBuffer {

	/**
	 * 单块editsLog缓冲区的最大大小：默认512字节
	 */
	public static final Integer EDIT_LOG_BUFFER_LIMIT = 25 * 1024;

	/**
	 * 是专门用来承载线程写入edits log
	 */
	private EditLogBuffer currentBuffer = new EditLogBuffer();
	/**
	 * 专门用来将数据同步到磁盘中去的一块缓冲
	 */
	private EditLogBuffer syncBuffer = new EditLogBuffer();

	// 上一次flush到磁盘的时候他的最大的txid是多少
	private long lastMaxTxid = 1L;

	/**
	 * 已经输入磁盘中的txid范围
	 */
	private List<String> flushedTxids = new ArrayList<>();

	/**
	 * 将edits log写到内存缓冲里去
	 * @param log
	 */
	public void write(EditLog log) {
		currentBuffer.write(log);
	}

	/**
	 * 交换两块缓冲区，为了同步内存数据到磁盘做准备
	 */
	public void setReadyToSync() {
		EditLogBuffer tmp = currentBuffer;
		currentBuffer = syncBuffer;
		syncBuffer = tmp;
	}

	/**
	 * 将syncBuffer缓冲区中的数据刷入磁盘中
	 */
	public void flush() {
		syncBuffer.flush();
		syncBuffer.clear();
	}

	/**
	 * 判断一下当前的缓冲区是否写满了需要刷到磁盘上去
	 */
	public boolean shouldSynToDisk() {
		if (currentBuffer.size() >= EDIT_LOG_BUFFER_LIMIT) {
			return true;
		}
		return false;
	}

	public List<String> getFlushedTxids() {
		return flushedTxids;
	}

	public String[] getBufferedEditsLog() {
		if (currentBuffer.size() == 0) {
			return null;
		}
		String editsLogRawData = new String(currentBuffer.getBufferData());
		return editsLogRawData.split("\n");
	}

	/**
	 * EditLog 缓冲区
	 */
	class EditLogBuffer {

		// 针对内存缓冲区的字节数组输出流
		private final ByteArrayOutputStream buffer;

		// 当前这块缓冲区写入的最大的一个txid
		long maxTxid = 0L;

		public EditLogBuffer() {
			this.buffer = new ByteArrayOutputStream(EDIT_LOG_BUFFER_LIMIT * 2);
		}

		/**
		 * 将 EditsLog 写入缓冲区
		 * @param log
		 */
		public void write(EditLog log) {
			this.maxTxid = log.getTxid();
			try {
				buffer.write(log.getContent().getBytes());
				buffer.write("\n".getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}

			System.out.println("当前缓冲区的大小是：" + size() +"，当前 txid = " + maxTxid);
		}

		/**
		 * 获取当前缓冲区已经写入数据的字节数量
		 * @return
		 */
		public Integer size() {
			return buffer.size();
		}

		public void flush() {
			byte[] data = buffer.toByteArray();
			ByteBuffer dataBuffer = ByteBuffer.wrap(data);

			String editsLogFilePath = "/Users/xiongtaolong/Documents/dfs/" + (lastMaxTxid) + "_" + maxTxid + ".log";

			flushedTxids.add(lastMaxTxid +"_"+ maxTxid);

			/**
			 * 需要注意的是，RandomAccessFile在这里并没有直接用于读写操作；相反，它是用来作为一个中介来创建FileOutputStream和FileChannel的。
			 * 然而，RandomAccessFile本身也提供了读写文件的能力，所以如果你不需要FileChannel的高级功能，也可以直接使用RandomAccessFile来进行文件操作。
			 */

			RandomAccessFile file = null;
			FileOutputStream out = null;
			FileChannel editsLogFileChannel = null;

			try {
				file = new RandomAccessFile(editsLogFilePath, "rw"); // 读写模式，数据写入缓冲区中

				/**
				 * file.getFD()：调用 RandomAccessFile 对象的 getFD() 方法。这个方法返回与 RandomAccessFile 关联的 FileDescriptor 对象。
				 * FileDescriptor 是一个抽象表示，它指代一个开放的文件、套接字或其他输入/输出资源。
				 *
				 * new FileOutputStream(file.getFD())：使用前面获得的 FileDescriptor 对象作为参数，来创建一个新的 FileOutputStream 对象。
				 * 通过这个方式，你并没有打开一个全新的文件或者创建一个新的文件描述符；相反，你创建了一个 FileOutputStream，
				 * 它共享了 RandomAccessFile 使用的相同文件描述符，从而操作同一个文件。
				 */
				out = new FileOutputStream(file.getFD());

				/**
				 * FileOutputStream 在这里主要用于获取与之关联的 FileChannel，因为 RandomAccessFile 本身并没有直接提供获取 FileChannel 的方法。
				 * 尽管 RandomAccessFile 也可以用于文件操作，但如果你需要利用 FileChannel 的特性（比如映射文件到内存，使用非阻塞I/O等），
				 * 则需要通过 FileOutputStream 或其他方式（比如直接通过文件路径）来获取 FileChannel。
				 *
				 * 另外，在大多数情况下，如果你不需要 FileChannel 的高级功能，直接使用 RandomAccessFile 就足够了。
				 * 但如果你确实需要 FileChannel，通过上述方式创建 FileOutputStream 并获取 FileChannel 是一个有效的方法。
				 */
				editsLogFileChannel = out.getChannel();

				/**
				 * 将 dataBuffer 这个 ByteBuffer 对象中的数据写入到与之关联的文件中。
				 * FileChannel 提供了一种在文件和内存之间传输数据的高效方式，它支持文件的直接读写操作，即数据可以直接从内存缓冲区传输到文件，
				 * 或者从文件传输到内存缓冲区，而不需要在用户和内核空间之间进行不必要的数据拷贝。
				 */
				editsLogFileChannel.write(dataBuffer);
				editsLogFileChannel.force(false);  // 强制把数据刷入磁盘上

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(out != null) {
						out.close();
					}
					if(file != null) {
						file.close();
					}
					if(editsLogFileChannel != null) {
						editsLogFileChannel.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			lastMaxTxid = maxTxid;
		}

		/**
		 * 清空掉内存缓冲里面的数据，复位
		 */
		public void clear() {
			buffer.reset();
		}

		public byte[] getBufferData() {
			return buffer.toByteArray();
		}
	}
}