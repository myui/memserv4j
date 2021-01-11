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
package memserv4j.util.net;

import memserv4j.util.lang.PoolableObjectFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ByteChannel;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PoolableSocketChannelFactory<E extends ByteChannel>
        implements PoolableObjectFactory<SocketAddress, E> {
    private static final Log LOG = LogFactory.getLog(PoolableSocketChannelFactory.class);

    private int sweepInterval = 60000;
    private int ttl = 30000;
    private int soRcvBufSize = -1;

    private final boolean datagram;
    private final boolean blocking;

    public PoolableSocketChannelFactory(boolean datagram, boolean blocking) {
        this.datagram = datagram;
        this.blocking = blocking;
    }

    public void configure(int sweepInterval, int ttl, int receiveBufferSize) {
        this.sweepInterval = sweepInterval;
        this.ttl = ttl;
        this.soRcvBufSize = receiveBufferSize;
    }

    @SuppressWarnings("unchecked")
    public E makeObject(SocketAddress sockAddr) {
        if (datagram) {
            return (E) createDatagramChannel(sockAddr, blocking);
        } else {
            return (E) createSocketChannel(sockAddr, blocking, soRcvBufSize);
        }
    }

    private static SocketChannel createSocketChannel(final SocketAddress sockAddr,
            final boolean blocking, final int rcvbufSize) {
        final SocketChannel ch;
        try {
            ch = SocketChannel.open();
            ch.configureBlocking(blocking);
        } catch (IOException e) {
            LOG.error("Failed to open SocketChannel.", e);
            throw new IllegalStateException(e);
        }
        final Socket sock = ch.socket();
        if (rcvbufSize != -1) {
            try {
                sock.setReceiveBufferSize(rcvbufSize);
            } catch (SocketException e) {
                LOG.error("Failed to setReceiveBufferSize.", e);
                throw new IllegalStateException(e);
            }
        }
        try {
            ch.connect(sockAddr);
        } catch (IOException e) {
            LOG.error("Failed to connect socket: " + sockAddr, e);
            throw new IllegalStateException(e);
        }
        return ch;
    }

    private static DatagramChannel createDatagramChannel(final SocketAddress sockAddr,
            final boolean blocking) {
        final DatagramChannel ch;
        try {
            ch = DatagramChannel.open();
            ch.configureBlocking(blocking);
        } catch (IOException e) {
            LOG.error("Failed to open DatagramChannel.", e);
            throw new IllegalStateException(e);
        }
        try {
            ch.socket().setBroadcast(false);
        } catch (SocketException e) {
            LOG.error("Failed to configure socket.", e);
            throw new IllegalStateException(e);
        }
        try {
            ch.connect(sockAddr);
        } catch (IOException e) {
            LOG.error("Failed to connect socket: " + sockAddr, e);
            throw new IllegalStateException(e);
        }
        return ch;
    }

    @Override
    public boolean validateObject(E sock) {
        if (sock == null) {
            return false;
        }
        return sock.isOpen();
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
    public boolean isValueCloseable() {
        return true;
    }

    @Override
    public Exception closeValue(E obj) {
        if (obj == null) {
            return null;
        }
        try {
            obj.close();
        } catch (IOException e) {
            return e;
        }
        return null;
    }

}
