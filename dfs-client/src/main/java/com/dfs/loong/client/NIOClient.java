package com.dfs.loong.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NIOClient {

    public static void sendFile(byte[] file, long fileSize) {
        SocketChannel channel = null;
        Selector selector = null;
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            channel.connect(new InetSocketAddress("localhost", 9000));
            selector = Selector.open();
            channel.register(selector, SelectionKey.OP_CONNECT);

            boolean sending = true;

            while (sending) {
                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();
                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();

                    if (key.isConnectable()) {
                        channel = (SocketChannel) key.channel();

                        // 检查SocketChannel是否还在尝试连接。在注册到Selector并收到OP_CONNECT事件后，你可能需要再次检查连接是否确实已完成，或者是否仍在尝试连接。
                        if (channel.isConnectionPending()) {

                            // 如果连接仍在尝试中，调用finishConnect()来完成连接。这个方法会阻塞，直到连接完成或抛出异常。但请注意，由于这个SocketChannel是在非阻塞模式下，所以实际上finishConnect()方法在这里应该是非阻塞的，并且会立即返回。
                            channel.finishConnect();

                            long imageLength = fileSize;

                            // 创建一个ByteBuffer，其大小是文件大小的两倍。但这里有个问题：通常，你不需要为文件内容分配两倍于其大小的空间，除非你有特殊的理由（例如，你要在发送之前对文件进行某种处理，这可能会增加其大小）。此外，将long值转换为int可能会导致溢出，如果文件大小超过Integer.MAX_VALUE。
                            ByteBuffer buffer = ByteBuffer.allocate((int) imageLength * 2);
                            buffer.putLong(imageLength); // long对应了8个字节，放到buffer里去
                            buffer.put(file);

                            // 将SocketChannel重新注册到Selector上，并监听OP_READ事件。或者监听OP_READ事件来准备接收对方的响应。
                            channel.register(selector, SelectionKey.OP_READ);
                        }
                    } else if (key.isReadable()) {
                        channel = (SocketChannel) key.channel();

                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int len = channel.read(buffer);

                        if (len > 0) {
                            System.out.println("[" + Thread.currentThread().getName()
                                    + "]收到响应：" + new String(buffer.array(), 0, len));
                            sending = false;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (selector != null) {
                try {
                    selector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
