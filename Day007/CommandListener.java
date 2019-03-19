package ru.croc.smartdata.spark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 *
 */
abstract class CommandListener implements Runnable {
    private final static org.apache.log4j.Logger log = org.apache.log4j.LogManager.getLogger(CommandListener.class);
    private static final String CMD_STOP = "STOP";
    private static final Long INTERVAL = 1000L;

    void exploreSocketChannel(int port) throws IOException, InterruptedException {
        try (ServerSocketChannel channel = ServerSocketChannel.open() ){
            channel.socket().bind(new InetSocketAddress(port));
            channel.configureBlocking(false);
            while(!tryReadStopCommand(channel)) {
                Thread.sleep(INTERVAL);
            }
        }
    }

    private boolean tryReadStopCommand(ServerSocketChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(128);
        SocketChannel socketChannel = channel.accept();
        if(socketChannel != null) {
            StringBuilder builder = new StringBuilder();
            int readAmount;
            while((readAmount = socketChannel.read(buffer)) >= 0) {
                if(readAmount > 0) {
                    builder.append(new String(buffer.array(), 0, readAmount));
                    log.error("Received >" + builder.toString() + "<");
                    if(builder.toString().contains(CMD_STOP)) {
                        return true;
                    }
                    buffer.clear();
                }
            }
        }
        return false;
    }
}
