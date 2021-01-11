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
package memserv4j.binary;

import memserv4j.MemcachedException;
import memserv4j.binary.BinaryProtocol.Header;
import memserv4j.binary.BinaryProtocol.Packet;
import memserv4j.binary.BinaryProtocol.ResponseStatus;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

public final class BinaryCommandDecoder extends FrameDecoder {

    public BinaryCommandDecoder() {
        super();
    }

    @Override
    protected Packet decode(ChannelHandlerContext cxt, Channel channel, ChannelBuffer buffer)
            throws Exception {
        if (buffer.readableBytes() < BinaryProtocol.HEADER_LENGTH) {
            return null;
        }

        int totalBody = buffer.getInt(8);
        int required = BinaryProtocol.HEADER_LENGTH + totalBody;
        if (buffer.readableBytes() < required) {
            return null;
        }

        Header header = new Header();
        header.decode(buffer);
        assert (header.totalBody == totalBody);

        if (header.magic != BinaryProtocol.MAGIC_BYTE_REQUEST) {
            channel.close();
            throw new MemcachedException(ResponseStatus.UNKNOWN,
                String.format("Invalid magic: %x\n", header.magic));
        }

        ChannelBuffer body = ChannelBuffers.buffer(totalBody);
        buffer.readBytes(body, totalBody);

        return new Packet(header, body);
    }

}
