package com.dfs.loong.datanode.server;

import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

@Service
public class DataNodeNIOServer extends Thread {

    private Selector selector;
    private final List<LinkedBlockingQueue<SelectionKey>> queues = new ArrayList<>();
    private final Map<String, CachedImage> cachedImages = new HashMap<>();

    static class CachedImage {

        String filename;
        long imageLength;
        long hasReadImageLength;

        public CachedImage(String filename, long imageLength, long hasReadImageLength) {
            this.filename = filename;
            this.imageLength = imageLength;
            this.hasReadImageLength = hasReadImageLength;
        }

        @Override
        public String toString() {
            return "CachedImage [filename=" + filename + ", imageLength=" + imageLength + ", hasReadImageLength="
                    + hasReadImageLength + "]";
        }

    }

    public DataNodeNIOServer() {
        ServerSocketChannel serverSocketChannel;

        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.socket().bind(new InetSocketAddress(9000), 100);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            for (int i = 0; i < 3; i++) {
                queues.add(new LinkedBlockingQueue<>());
            }

            for (int i = 0; i < 3; i++) {
                new Worker(queues.get(i)).start();
            }

            System.out.println("NIOServer已经启动，开始监听端口：" + 9000);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        while (true) {
            try {
                selector.select();
                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()) {
                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();
                    handleRequest(key);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private void handleRequest(SelectionKey key)
            throws IOException {
        SocketChannel channel = null;

        try {
            // 表示有新的连接请求，即key.isAcceptable()返回true。
            if (key.isAcceptable()) {
                ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                channel = serverSocketChannel.accept();
                if (channel != null) {
                    channel.configureBlocking(false);
                    channel.register(selector, SelectionKey.OP_READ);
                }
                // 表示数据可读。这可以应用于任何类型的通道，比如套接字通道（SocketChannel）或文件通道（FileChannel），当它们有数据可供读取时。
            } else if (key.isReadable()) {
                channel = (SocketChannel) key.channel();
                String remoteAddr = channel.getRemoteAddress().toString();
                int queueIndex = remoteAddr.hashCode() % queues.size();
                queues.get(queueIndex).put(key);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            if (channel != null) {
                channel.close();
            }
        }
    }

    class Worker extends Thread {

        private final LinkedBlockingQueue<SelectionKey> queue;

        public Worker(LinkedBlockingQueue<SelectionKey> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            while (true) {
                SocketChannel channel = null;

                try {
                    SelectionKey key = queue.take();

                    channel = (SocketChannel) key.channel();
                    if (!channel.isOpen()) {
                        channel.close();
                        continue;
                    }

                    // 获取与SocketChannel关联的远程地址，并将其转换为字符串。
                    String remoteAddr = channel.getRemoteAddress().toString();

                    // 为读取数据分配一个大小为10KB的ByteBuffer。
                    ByteBuffer buffer = ByteBuffer.allocate(10 * 1024);
                    int len = -1;

                    String filename = null;
                    if (cachedImages.containsKey(remoteAddr)) {
                        filename = cachedImages.get(remoteAddr).filename;
                    } else {
                        filename = "F:\\development\\tmp\\" + UUID.randomUUID().toString() + ".jpg";
                    }

                    long imageLength = 0;

                    if (cachedImages.containsKey(remoteAddr)) {
                        imageLength = cachedImages.get(remoteAddr).imageLength;
                    } else {
                        // 从通道中读取图像长度
                        len = channel.read(buffer);
                        buffer.flip();
                        if (len > 8) {
                            byte[] imageLengthBytes = new byte[8];

                            // 取前八个字节，表示本次文件流的长度
                            buffer.get(imageLengthBytes, 0, 8);

                            ByteBuffer imageLengthBuffer = ByteBuffer.allocate(8);
                            imageLengthBuffer.put(imageLengthBytes);
                            imageLengthBuffer.flip();
                            imageLength = imageLengthBuffer.getLong();
                        } else if (len <= 0) {
                            channel.close();
                            continue;
                        }
                    }

                    long hasReadImageLength = 0;
                    if (cachedImages.containsKey(remoteAddr)) {
                        hasReadImageLength = cachedImages.get(remoteAddr).hasReadImageLength;
                    }

                    FileOutputStream imageOut = new FileOutputStream(filename);
                    FileChannel imageChannel = imageOut.getChannel();

                    // 这行代码实际上是多余的，因为当你首次打开一个文件时，position默认就是0（文件的开始）。但如果文件已存在并且你希望追加数据，这个调用会是有意义的。但在这里，它似乎没有实际作用，除非文件之前已经被写入过数据。
                    imageChannel.position(imageChannel.size());

                    if (!cachedImages.containsKey(remoteAddr)) {
                        hasReadImageLength += imageChannel.write(buffer);
                        buffer.clear();
                    }

                    while ((len = channel.read(buffer)) > 0) {
                        hasReadImageLength += len;
                        buffer.flip();
                        imageChannel.write(buffer);
                        buffer.clear();
                    }

                    // 如果缓存中存在远程地址，并且已经读取了与缓存中相同的字节数，那么关闭SocketChannel并继续下一次循环。
                    if (cachedImages.get(remoteAddr) != null) {
                        if (hasReadImageLength == cachedImages.get(remoteAddr).hasReadImageLength) {
                            channel.close();
                            continue;
                        }
                    }

                    imageChannel.close();
                    imageOut.close();

                    // 如果接收到的字节数等于预期的图像长度，那么发送一个“SUCCESS”消息给发送方，并从缓存中移除该远程地址。否则，创建一个CachedImage对象来保存当前的状态，并将其添加到缓存中。
                    if (hasReadImageLength == imageLength) {
                        ByteBuffer outBuffer = ByteBuffer.wrap("SUCCESS".getBytes());
                        channel.write(outBuffer);
                        cachedImages.remove(remoteAddr);
                    } else {
                        CachedImage cachedImage = new CachedImage(filename, imageLength, hasReadImageLength);
                        cachedImages.put(remoteAddr, cachedImage);
                        key.interestOps(SelectionKey.OP_READ);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    if (channel != null) {
                        try {
                            channel.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        }
    }
}
