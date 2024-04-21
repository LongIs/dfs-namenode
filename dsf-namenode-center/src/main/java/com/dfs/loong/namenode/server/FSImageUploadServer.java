package com.dfs.loong.namenode.server;

import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

/**
 * 负责fsimage文件上传的server
 *
 * @author zhonghuashishan
 */
@Service
public class FSImageUploadServer extends Thread {

    private Selector selector;

    public FSImageUploadServer() {
        this.init();
    }

    /**
     * 套接字（Socket）是计算机网络中用于实现不同主机之间进程通信的一种技术。它提供了一种标准化的接口，使得不同主机上的应用程序可以通过网络进行数据的发送和接收。
     * 套接字可以被视为网络通信的一个端点，它允许应用程序在网络上创建连接、发送数据以及接收数据。通过套接字，不同主机上的应用程序可以建立连接并进行通信，就好像它们在同一台机器上一样。
     */
    private void init() {
        // 用于存储服务器套接字通道对象
        // 通过这个通道，服务器可以打开并管理一个或多个套接字，以便与客户端进行通信。
        // 简单来说，“服务器套接字通道”就是服务器上用于监听和接受客户端连接请求，并与客户端进行通信的一个通道或接口。它使得服务器能够同时处理多个客户端的连接请求，实现高效的网络通信。
        // 在Java NIO中，ServerSocketChannel 是一个可以打开服务器套接字的通道。这个通道允许服务器监听进来的TCP连接，当客户端尝试连接到服务器时，服务器通过 ServerSocketChannel 接受这些连接。
        ServerSocketChannel serverSocketChannel;
        try {

            // 调用Selector.open()方法创建一个新的选择器对象，并将其赋值给之前声明的selector变量。
            selector = Selector.open();
            // 打开一个新的服务器套接字通道。这个通道将用于监听新的TCP连接请求。
            serverSocketChannel = ServerSocketChannel.open();
            // 将服务器套接字通道设置为非阻塞模式。这意味着通道上的I/O操作将不会阻塞调用线程，而是立即返回。如果设置为阻塞模式（configureBlocking(true)），则I/O操作会阻塞调用线程，直到操作完成。
            serverSocketChannel.configureBlocking(false);
            // 绑定IP和端口，默认 0.0.0.0，
            // bind()方法的第二个参数100是设置了一个背压参数（backlog），它表示操作系统可以挂起的、尚未接受的最大连接数。
            serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            // 将服务器套接字通道注册到之前创建的选择器selector上，并指定它感兴趣的操作是接受新的连接（SelectionKey.OP_ACCEPT）。
            // 这意味着当有新的连接请求到达时，选择器会将其标记为就绪，以便程序可以稍后通过选择器来检查并接受这些连接。
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        System.out.println("FSImageUploadServer启动，监听9000端口......");

        while (true) {
            try {
                // 调用Selector的select()方法，它会阻塞直到至少有一个通道已准备好进行I/O操作，或者当前线程被中断，或者超时（如果在调用select()时指定了超时时间）。
                // 一旦select()方法返回，表示至少有一个通道的状态发生了变化，比如变得可读或可写。
                selector.select();
                // 这行代码获取了Selector上所有已准备好进行I/O操作的通道的SelectionKey集合，并创建了一个迭代器来遍历这些键。
                // 每个SelectionKey代表一个已注册到Selector上的通道及其感兴趣的操作（读、写、连接等）。
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                // 用于遍历所有已准备好进行I/O操作的SelectionKey。
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();

                    // 从Selector的已选择键集中移除当前SelectionKey。这一步很重要，因为如果不手动移除，下一次调用select()方法时，
                    // 相同的键可能仍然会存在于集合中，这可能导致处理重复或者不正确的逻辑。
                    keysIterator.remove();
                    try {
                        handleRequest(key);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private void handleRequest(SelectionKey key) throws IOException {
        // 表示有新的连接请求，即key.isAcceptable()返回true。
		// 这通常发生在服务器套接字通道（ServerSocketChannel）上，当客户端尝试连接时。
        if (key.isAcceptable()) {
            handleConnectRequest(key);

            // 表示数据可读。这可以应用于任何类型的通道，比如套接字通道（SocketChannel）或文件通道（FileChannel），当它们有数据可供读取时。
        } else if (key.isReadable()) {
            handleReadableRequest(key);

            // 表示通道现在可以写入数据。同样，这适用于任何类型的通道，特别是当你想发送数据到客户端时（比如SocketChannel）。
        } else if (key.isWritable()) {
            handleWritableRequest(key);
        }
    }

    /**
     * 处理BackupNode连接请求
     *
     * @param key
     * @throws Exception
     */
    private void handleConnectRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

            // 调用 accept() 方法接受新的连接请求，并返回一个新的SocketChannel对象，代表与客户端的连接。如果这个调用成功（即没有立即返回null），则新的SocketChannel被赋值给channel变量。
            channel = serverSocketChannel.accept();
            if (channel != null) {
            	// 将新接受的SocketChannel设置为非阻塞模式。在NIO中，非阻塞模式允许通道在没有数据可读或可写时立即返回，而不是阻塞调用线程。
                channel.configureBlocking(false);
                // 将新接受的SocketChannel注册到原始的Selector对象上，并指定对读取操作感兴趣。这意味着一旦有数据可以从这个SocketChannel读取，
				// Selector的select()方法就会返回，并且这个SocketChannel的SelectionKey将被添加到selectedKeys集合中。
                channel.register(selector, SelectionKey.OP_READ);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (channel != null) {
                channel.close();
            }
        }
    }

    /**
     * 处理发送fsimage文件的请求
     *
     * @param key
     * @throws Exception
     */
    private void handleReadableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            String fsimageFilePath = "/Users/xiongtaolong/Documents/dfs/fsimage.meta";

            RandomAccessFile fsimageImageRAF = null;
            FileOutputStream fsimageOut = null;
            FileChannel fsimageFileChannel = null;

            try {
                channel = (SocketChannel) key.channel();
                ByteBuffer buffer = ByteBuffer.allocate(1024);

                int total = 0;
                int count = -1;

                if ((count = channel.read(buffer)) > 0) {
                    File file = new File(fsimageFilePath);
                    if (file.exists()) {
                        file.delete();
                    }

                    fsimageImageRAF = new RandomAccessFile(fsimageFilePath, "rw");
                    fsimageOut = new FileOutputStream(fsimageImageRAF.getFD());
                    fsimageFileChannel = fsimageOut.getChannel();

                    total += count;

                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                } else {
                    channel.close();
                }

                while ((count = channel.read(buffer)) > 0) {
                    total += count;
                    buffer.flip();
                    fsimageFileChannel.write(buffer);
                    buffer.clear();
                }

                if (total > 0) {
                    System.out.println("接收fsimage文件以及写入本地磁盘完毕......");
                    fsimageFileChannel.force(false);
                    channel.register(selector, SelectionKey.OP_WRITE);
                }
            } finally {
                if (fsimageOut != null) {
                    fsimageOut.close();
                }
                if (fsimageImageRAF != null) {
                    fsimageImageRAF.close();
                }
                if (fsimageFileChannel != null) {
                    fsimageFileChannel.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (channel != null) {
                channel.close();
            }
        }
    }

    /**
     * 处理返回响应给BackupNode
     * 当 handleReadableRequest 方法执行到最后一行，会 register OP_WRITE
     * @param key
     * @throws Exception
     */
    private void handleWritableRequest(SelectionKey key) throws IOException {
        SocketChannel channel = null;

        try {
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            buffer.put("SUCCESS".getBytes());
            buffer.flip();

            channel = (SocketChannel) key.channel();
            channel.write(buffer);

            System.out.println("fsimage上传完毕，返回响应SUCCESS给backupnode......");

            channel.register(selector, SelectionKey.OP_READ);
        } catch (Exception e) {
            e.printStackTrace();
            if (channel != null) {
                channel.close();
            }
        }
    }

}
