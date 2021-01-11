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
package memserv4j.util.netty;

import memserv4j.util.lang.PoolableObjectFactory;

import java.net.SocketAddress;

import javax.annotation.Nonnull;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;

public final class NettyChannelFutureFactory
        implements PoolableObjectFactory<SocketAddress, ChannelFuture> {

    @Nonnull
    private final ClientBootstrap cb;
    private int sweepInterval = 60000;
    private int ttl = 30000;

    public NettyChannelFutureFactory(@Nonnull ClientBootstrap cb) {
        this.cb = cb;
    }

    public NettyChannelFutureFactory(@Nonnull ClientBootstrap cb, int sweepInterval, int ttl) {
        this.cb = cb;
        this.sweepInterval = sweepInterval;
        this.ttl = ttl;
    }

    @Override
    public int getSweepInterval() {
        return sweepInterval;
    }

    @Override
    public int getTimeToLive() {
        return ttl;
    }

    @Override
    public ChannelFuture makeObject(SocketAddress sockAddr) {
        ChannelFuture f = cb.connect(sockAddr);
        return f;
    }

    @Override
    public boolean validateObject(ChannelFuture f) {
        if (f == null) {
            return false;
        }
        return f.getChannel().isOpen();
    }

    @Override
    public boolean isValueCloseable() {
        return true;
    }

    @Override
    public Exception closeValue(ChannelFuture f) {
        if (f != null) {
            f.getChannel().close();
        }
        return null;
    }

}
