/*
 * Copyright 2019 and onwards Makoto Yui
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package memserv4j.proxy;

import memserv4j.MemcachedException;
import memserv4j.Settings;
import memserv4j.binary.BinaryPipelineFactory;
import memserv4j.util.concurrent.ExecutorFactory;
import memserv4j.util.lang.Primitives;
import memserv4j.util.net.NetUtils;

import java.net.InetSocketAddress;

import javax.annotation.Nonnull;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

public abstract class MemcachedProxyServer {

    public MemcachedProxyServer() {}

    public void start() throws MemcachedException {
        final ChannelFactory channelFactory = new NioServerSocketChannelFactory(
            ExecutorFactory.newCachedThreadPool("memcached-gateway-server-boss"),
            ExecutorFactory.newCachedThreadPool("memcached-gateway-server-worker"));
        final ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
        bootstrap.setOption("child.tcpNoDelay", true); // recommended
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.keepAlive", true);

        final ChannelGroup acceptedChannels = new DefaultChannelGroup("all_client_connections");
        ChannelHandler handler = getCommandProxy();
        bootstrap.setPipelineFactory(new BinaryPipelineFactory(handler));

        int port = Primitives.parseInt(Settings.get("memserv4j.gateway.port"), 11211);
        final Channel serverChannel =
                bootstrap.bind(new InetSocketAddress(NetUtils.getLocalHost(), port));

        Runnable shutdownRunnable = new Runnable() {
            public void run() {
                serverChannel.close().awaitUninterruptibly(); // close server socket
                acceptedChannels.close().awaitUninterruptibly(); // close client connections
                channelFactory.releaseExternalResources(); // stop the boss and worker threads of ChannelFactory
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownRunnable));
    }

    @Nonnull
    protected abstract BinaryCommandProxy getCommandProxy();

    public void stop() throws MemcachedException {}
}
