package com.wentong;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test1 {

    public static void main(String[] args) throws Exception {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup();
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap().group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ch.pipeline().addLast(new EchoServerHandler());
                }
            });
            ChannelFuture future = bootstrap.bind(8080).sync();
            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    static class EchoServerHandler extends ChannelInboundHandlerAdapter {

        static ExecutorService executorService = Executors.newSingleThreadExecutor();
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf reqMsg = (ByteBuf) msg;
            byte[] body = new byte[reqMsg.readableBytes()];
            reqMsg.readBytes(body);
            executorService.execute(() -> {
                //解析请求消息，做路由转发，代码省略
                //转发成功，返回响应给客户端
                ByteBuf respMsg = allocator.heapBuffer(body.length);
                respMsg.writeBytes(body);//作为示例，简化处理，将请求返回
                System.out.println("write back to client: " + new String(body));
                ctx.writeAndFlush(respMsg);
            });
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}

