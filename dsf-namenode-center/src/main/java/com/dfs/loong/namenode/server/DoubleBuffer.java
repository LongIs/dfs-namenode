package com.dfs.loong.namenode.server;

/**
 * 内存双缓冲
 * @author zhonghuashishan
 *
 */
public class DoubleBuffer {

	/**
	 * 单块editsLog缓冲区的最大大小：默认512字节
	 */
	public static final Long EDIT_LOG_BUFFER_LIMIT = 512 * 1024L;

	/**
	 * 是专门用来承载线程写入edits log
	 */
	private EditLogBuffer currentBuffer = new EditLogBuffer();
	/**
	 * 专门用来将数据同步到磁盘中去的一块缓冲
	 */
	private EditLogBuffer syncBuffer = new EditLogBuffer();

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

	/**
	 * EditLog 缓冲区
	 */
	class EditLogBuffer {

		/**
		 * 将 EditsLog 写入缓冲区
		 * @param log
		 */
		public void write(EditLog log) {

		}

		/**
		 * 获取当前缓冲区已经写入数据的字节数量
		 * @return
		 */
		public Long size() {
			return 0L;
		}

		public void flush() {

		}

		public void clear() {

		}
	}
}