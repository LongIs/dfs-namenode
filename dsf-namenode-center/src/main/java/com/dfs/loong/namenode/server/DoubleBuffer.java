package com.dfs.loong.namenode.server;

import com.dfs.loong.namenode.vo.EditLog;
import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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


	public static void main(String[] args) {
		//String s = "taskId=null, taskTitle=咨询留言审核-房屋租赁合同无法上传发票号, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031311614901, taskRequestId=102024031311614901, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031311614901&rwdxdah=110000000085723808, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Wed Mar 13 20:45:59 CST 2024, sendTime=Mon Mar 18 10:36:39 CST 2024, receiveTime=Mon Mar 18 10:36:39 CST 2024, taskDeadline=Wed Mar 20 20:45:59 CST 2024, completionTime=null, lySwryDm=24403068752, lySwrymc=殷振兴, lyGwDm=null, lyGwmc=null, lySwjgDm=14403312600, lySwjgmc=国家税务总局深圳市龙华区税务局观湖税务所, blDxlx=04, blDxDm=14403312600-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=14403312600, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=b3c3d674002f450d8e07baeefe051f65, requestId=a23a3613d32f44c0944e2e128b5118a6, sjgsdq=14403000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:36:39 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Wed Mar 20 20:45:59 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";
		//String s = "taskId=null, taskTitle=咨询留言审核-房屋租赁合同无法上传发票号, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031311614901, taskRequestId=102024031311614901, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031311614901&rwdxdah=110000000085723808, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Wed Mar 13 20:45:59 CST 2024, sendTime=Mon Mar 18 10:36:39 CST 2024, receiveTime=Mon Mar 18 10:36:39 CST 2024, taskDeadline=Wed Mar 20 20:45:59 CST 2024, completionTime=null, lySwryDm=24403068752, lySwrymc=殷振兴, lyGwDm=null, lyGwmc=null, lySwjgDm=14403312600, lySwjgmc=国家税务总局深圳市龙华区税务局观湖税务所, blDxlx=04, blDxDm=14403312600-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=14403312600, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=b3c3d674002f450d8e07baeefe051f65, requestId=a23a3613d32f44c0944e2e128b5118a6, sjgsdq=14403000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:36:39 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Wed Mar 20 20:45:59 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";
		//String s = "taskId=null, taskTitle=咨询留言审核-离婚，专项扣除被对方全额操作, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611672556, taskRequestId=102024031611672556, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031611672556&rwdxdah=110000000019191755, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 03:31:12 CST 2024, sendTime=Mon Mar 18 10:38:38 CST 2024, receiveTime=Mon Mar 18 10:38:38 CST 2024, taskDeadline=Mon Mar 25 03:31:12 CST 2024, completionTime=null, lySwryDm=14429331979, lySwrymc=杨彪, lyGwDm=null, lyGwmc=null, lySwjgDm=14403065108, lySwjgmc=国家税务总局深圳市宝安区税务局西乡税务所, blDxlx=04, blDxDm=14403065108-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=14403065108, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=c62564ba891b4491877b58305c57f9d3, requestId=e607eecc0cdf419b8424bd04f5bdce87, sjgsdq=14403000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:38:38 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Mon Mar 25 03:31:12 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";
		// String s= "taskId=null, taskTitle=咨询留言审核-你好，我修改了22年的申请退税信息，是否需要补税呢, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031311560054, taskRequestId=102024031311560054, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031311560054&rwdxdah=110000000014351135, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Wed Mar 13 00:44:32 CST 2024, sendTime=Mon Mar 18 10:36:41 CST 2024, receiveTime=Mon Mar 18 10:36:41 CST 2024, taskDeadline=Wed Mar 20 00:44:32 CST 2024, completionTime=null, lySwryDm=14429331466, lySwrymc=黄玉标, lyGwDm=null, lyGwmc=null, lySwjgDm=14403310800, lySwjgmc=国家税务总局深圳市龙华区税务局第二税务所, blDxlx=04, blDxDm=14403310800-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=14403310800, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=cc6f15c034de4ed9a3ffe920dfad7315, requestId=e15f6c7c0d5345aaaf4e22295c2a856d, sjgsdq=14403000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:36:41 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Wed Mar 20 00:44:32 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";


		//String s = "taskId=null, taskTitle=咨询留言审核-本应退2924.75，显示退0元, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031311582368, taskRequestId=102024031311582368, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031311582368&rwdxdah=110000000393881021, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Wed Mar 13 09:03:14 CST 2024, sendTime=Mon Mar 18 10:36:38 CST 2024, receiveTime=Mon Mar 18 10:36:38 CST 2024, taskDeadline=Wed Mar 20 09:03:14 CST 2024, completionTime=null, lySwryDm=13301093835, lySwrymc=赵晨雨, lyGwDm=null, lyGwmc=null, lySwjgDm=13301099200, lySwjgmc=国家税务总局杭州市萧山区税务局经济技术开发区税务所, blDxlx=04, blDxDm=13301099200-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=13301099200, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=17d39a3d4742443781a9dae8bde69cd4, requestId=53457e053a8343b887c552699306c2d2, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:36:38 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Wed Mar 20 09:03:14 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";
		//String s = "taskId=null, taskTitle=咨询留言解答-怎样知道专项税扣除是否计入, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611660297, taskRequestId=102024031611660297, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611660297&rwdxdah=110000000502837045, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 09:26:00 CST 2024, sendTime=Mon Mar 18 11:01:32 CST 2024, receiveTime=Mon Mar 18 11:01:32 CST 2024, taskDeadline=Mon Mar 25 09:26:00 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=51c9606686b74f19ab90e8ee96316741, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 11:01:32 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 09:26:00 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true";

		/*List<String> list = Lists.newArrayList("taskId=null, taskTitle=咨询留言解答-如何退税, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611676193, taskRequestId=102024031611676193, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611676193&rwdxdah=200000000131256791, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 15:31:05 CST 2024, sendTime=Mon Mar 18 10:40:23 CST 2024, receiveTime=Mon Mar 18 10:40:23 CST 2024, taskDeadline=Mon Mar 25 15:31:05 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=cf1a788bb0e84c569cccdb71f9b3d00a, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:40:23 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 15:31:05 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true",
				"taskId=null, taskTitle=咨询留言解答-无合同如何填报免税, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611678376, taskRequestId=102024031611678376, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611678376&rwdxdah=110000000195371665, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 08:43:17 CST 2024, sendTime=Mon Mar 18 10:45:08 CST 2024, receiveTime=Mon Mar 18 10:45:08 CST 2024, taskDeadline=Mon Mar 25 08:43:17 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=e956897fb30d4401b281fc8af9bbe912, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:45:08 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 08:43:17 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true",
				"taskId=null, taskTitle=咨询留言解答-退税, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611686391, taskRequestId=102024031611686391, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611686391&rwdxdah=110000000094532183, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 15:29:27 CST 2024, sendTime=Mon Mar 18 10:41:23 CST 2024, receiveTime=Mon Mar 18 10:41:23 CST 2024, taskDeadline=Mon Mar 25 15:29:27 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=fe835369bb5c49d6b7db676e4d8086a5, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:41:23 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 15:29:27 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true",
				"taskId=null, taskTitle=咨询留言解答-退税流程咨询, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611715893, taskRequestId=102024031611715893, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611715893&rwdxdah=110000000098530464, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 16:46:04 CST 2024, sendTime=Mon Mar 18 10:41:35 CST 2024, receiveTime=Mon Mar 18 10:41:35 CST 2024, taskDeadline=Mon Mar 25 16:46:04 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=4a83815ec5054f3d9694ae63849c1da3, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:41:35 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 16:46:04 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true",
				"taskId=null, taskTitle=咨询留言解答-操作错误了 , taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611725919, taskRequestId=102024031611725919, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120021&rwdh=102024031611725919&rwdxdah=110000000460952759, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 20:42:59 CST 2024, sendTime=Mon Mar 18 10:41:47 CST 2024, receiveTime=Mon Mar 18 10:41:47 CST 2024, taskDeadline=Mon Mar 25 20:42:59 CST 2024, completionTime=null, lySwryDm=13301100514, lySwrymc=张晓洁, lyGwDm=null, lyGwmc=null, lySwjgDm=13301134400, lySwjgmc=国家税务总局杭州市临平区税务局第二税务所, blDxlx=03, blDxDm=13301100514, gwxh=null, blDxmc=张晓洁, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=102001, blGwmc=咨询任务处理岗, taskKey=f33803dceecd4cfeb9852280251bdddd, requestId=1676a6f5ec70446997cb9e8a095f3617, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:41:47 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Mon Mar 25 20:42:59 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true");
*/

		// List<String> list = Lists.newArrayList("taskId=null, taskTitle=咨询留言审核-【中国税务】尊敬的纳税人冯昌佩：如您近期修改或删除过以往年度个人所得税专项附加扣除信息，请同步对以往, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031511682896, taskRequestId=102024031511682896, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031511682896&rwdxdah=110000000637606968, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Fri Mar 15 18:14:17 CST 2024, sendTime=Mon Mar 18 10:38:39 CST 2024, receiveTime=Mon Mar 18 10:38:39 CST 2024, taskDeadline=Fri Mar 22 18:14:17 CST 2024, completionTime=null, lySwryDm=53303810010, lySwrymc=朱温臣, lyGwDm=null, lyGwmc=null, lySwjgDm=13303813100, lySwjgmc=国家税务总局瑞安市税务局安阳税务分局, blDxlx=04, blDxDm=13303813100-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=13303813100, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=a73e65cc9d19469aaa96b0d9f344c6a6, requestId=29daedf257ec4d8bbed492e266547aab, sjgsdq=13300000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:38:39 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Fri Mar 22 18:14:17 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true");

		/*List<String> list = Lists.newArrayList("askId=null, taskTitle=异议申诉任务-广西钦州市宏亮货运代理有限公司-91450706MACLCXN42L, taskDescription=null, taskSystemSource=SWZJ-FXGL, taskSystemSourceName=null, taskBusinessId=602024031815729896, taskRequestId=602024031815729896, taskFlDm=SXD05200300101, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/fx/fxgl/view/fxyd/rwcl/yyss.html?rwxh=1000000018358363&nsrdah=10214500000000389731, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Mon Mar 18 03:50:01 CST 2024, sendTime=Mon Mar 18 10:36:38 CST 2024, receiveTime=Mon Mar 18 10:36:38 CST 2024, taskDeadline=Wed Apr 17 03:50:01 CST 2024, completionTime=null, lySwryDm=24507000218, lySwrymc=曾慕瑶, lyGwDm=null, lyGwmc=null, lySwjgDm=14570001400, lySwjgmc=国家税务总局中国（广西）自由贸易试验区钦州港片区税务局第三税务分局, blDxlx=03, blDxDm=14507040052, gwxh=null, blDxmc=梁安琪, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=602001, blGwmc=管理服务任务应对岗, taskKey=69c11319a62e48a6a02c77095e77944f, requestId=100fe5ba9d494cc980f7cbfbdd380e23, sjgsdq=14500000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:36:38 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Wed Apr 17 03:50:01 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true",
				"taskId=null, taskTitle=异议申诉任务-广西桂灵钦武商贸有限公司-91450706MA5Q4F7A97, taskDescription=null, taskSystemSource=SWZJ-FXGL, taskSystemSourceName=null, taskBusinessId=602024031715650423, taskRequestId=602024031715650423, taskFlDm=SXD05200300101, ywlxMc=null, taskStatus=02, taskStatusExplain=人工分配, blUrl=/fx/fxgl/view/fxyd/rwcl/yyss.html?rwxh=1000000018477703&nsrdah=10214500000000057465, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sun Mar 17 00:40:01 CST 2024, sendTime=Mon Mar 18 10:34:48 CST 2024, receiveTime=Mon Mar 18 10:34:48 CST 2024, taskDeadline=Tue Apr 16 00:40:01 CST 2024, completionTime=null, lySwryDm=24507000218, lySwrymc=曾慕瑶, lyGwDm=null, lyGwmc=null, lySwjgDm=14570001400, lySwjgmc=国家税务总局中国（广西）自由贸易试验区钦州港片区税务局第三税务分局, blDxlx=03, blDxDm=54570000019, gwxh=null, blDxmc=吴林冰, blSwjgDm=00000000000, blSwjgmc=国家税务总局, blGwDm=602001, blGwmc=管理服务任务应对岗, taskKey=a4c18d7944de45c487b21f393d7b74d7, requestId=100fe5ba9d494cc980f7cbfbdd380e23, sjgsdq=14500000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:34:48 CST 2024, yxbz=null, readStatus=null, activityName=已分配, businessDeadline=Tue Apr 16 00:40:01 CST 2024, buttons=null, attachments=null, priority=0, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true");*/


		List<String> list = Lists.newArrayList("taskId=null, taskTitle=咨询留言审核-离婚，专项扣除被对方全额操作, taskDescription=null, taskSystemSource=SWZJ-DZGZPTZNHD, taskSystemSourceName=null, taskBusinessId=102024031611672556, taskRequestId=102024031611672556, taskFlDm=A600120002, ywlxMc=null, taskStatus=02, taskStatusExplain=处理办结, blUrl=/commonbusiness/view/yzq.html?ywlxDm=A600120024&rwdh=102024031611672556&rwdxdah=110000000019191755, taskFlowchartAddr=null, taskBusinessCreator=null, createTime=Sat Mar 16 03:31:12 CST 2024, sendTime=Mon Mar 18 10:38:38 CST 2024, receiveTime=Mon Mar 18 10:38:38 CST 2024, taskDeadline=Mon Mar 25 03:31:12 CST 2024, completionTime=null, lySwryDm=14429331979, lySwrymc=杨彪, lyGwDm=null, lyGwmc=null, lySwjgDm=14403065108, lySwjgmc=国家税务总局深圳市宝安区税务局西乡税务所, blDxlx=04, blDxDm=14403065108-G00000000099, gwxh=null, blDxmc=咨询任务审核岗, blSwjgDm=14403065108, blSwjgmc=国家税务总局, blGwDm=103001, blGwmc=咨询任务审核岗, taskKey=c62564ba891b4491877b58305c57f9d3, requestId=e607eecc0cdf419b8424bd04f5bdce87, sjgsdq=14403000000, sjxgdq=null, lrrDm=99999999999, xgrDm=null, xgrq=null, lrrq=Mon Mar 18 10:38:38 CST 2024, yxbz=null, readStatus=null, activityName=待审核, businessDeadline=Mon Mar 25 03:31:12 CST 2024, buttons=null, attachments=null, priority=null, remindDay=null, labelId=null, retryBz=false, cancelBz=false, writeRecord=true");


		SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		for (String s : list) {
			String[] split = s.split(",");
			Map<String, Object> map = new HashMap<>();
			for (String item : split) {
				String split1 = item.substring(0, item.indexOf("="));
				Object value = item.substring(item.indexOf("=") + 1);
				if ("null".equals(value)) {
					value = null;
				}
				map.put(split1.trim(), value);
			}

			TaskInfoDO o = new TaskInfoDO();
			// 获取对象所有字段
			Field[] fields = o.getClass().getDeclaredFields();

			StringBuilder columns = new StringBuilder();
			StringBuilder values = new StringBuilder();



			// 遍历字段，转换为下划线命名
			for (Field field : fields) {
				field.setAccessible(true); // 使得私有字段也可以访问
				String fieldName = field.getName();
				Object value = map.get(fieldName);

				String columnName = toUnderScoreCase(fieldName);

				try {
					if (value == null) {
						value = field.get(o);
						if ("xgrq".equals(columnName)) {
							value = map.get("lrrq");
						}
					}
					switch (field.getType().getName()) {
						case "java.util.Date":
							columns.append(columnName).append(", ");

							if (value == null) {
								values.append(value).append(", ");
							} else {

								try {
									Date date = sdf.parse((String) value);
									values.append("'").append(simpleDateFormat.format(date)).append("'").append(", ");
								} catch (ParseException e) {
									e.printStackTrace();
								}
							}
							break;
						case "java.lang.String":
							columns.append(columnName).append(", ");
							if (value == null) {
								values.append(value).append(", ");
							} else {
								values.append("".equals(value) ? "''" : "'" +  ((String) value).replaceAll("'", "\\\\'") + "'").append(", ");
							}

							break;
						default:
							columns.append(columnName).append(", ");
							if (columnName.equals("task_id")) {
								value = "AUTO_SEQ_RWZX.nextval";
							}
							values.append(value).append(", ");
							break;
					}

				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			String s1 = "insert into task_info ";
			String fieldsStr = columns.toString().substring(0, columns.toString().length() - 2);
			String valuesStr = values.toString().substring(0, values.toString().length() - 2);
			System.out.println(s1 + "(" + fieldsStr + ") values (" + valuesStr + ")");
		}



	}

	// 将驼峰命名转换为下划线命名
	private static String toUnderScoreCase(String camelCaseName) {
		StringBuilder sb = new StringBuilder();
		char[] chars = camelCaseName.toCharArray();
		for (char c : chars) {
			if (Character.isUpperCase(c)) {
				sb.append("_").append(Character.toLowerCase(c));
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}
}