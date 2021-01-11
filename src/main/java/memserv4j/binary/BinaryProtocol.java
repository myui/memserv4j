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

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @link http://code.google.com/p/memcached/wiki/MemcacheBinaryProtocol
 */
public final class BinaryProtocol {

    public static final int HEADER_LENGTH = 24;

    //  Magic Byte
    public static final byte MAGIC_BYTE_REQUEST = (byte) 0x80;
    public static final byte MAGIC_BYTE_RESPONSE = (byte) 0x81;

    // Command Opcodes (1 byte)
    public static final byte OPCODE_GET = 0x00;
    public static final byte OPCODE_SET = 0x01;
    public static final byte OPCODE_ADD = 0x02;
    public static final byte OPCODE_REPLACE = 0x03;
    public static final byte OPCODE_DELETE = 0x04;
    public static final byte OPCODE_INCREMENT = 0x05;
    public static final byte OPCODE_DECREMENT = 0x06;
    public static final byte OPCODE_QUIT = 0x07;
    public static final byte OPCODE_FLUSH = 0x08;
    public static final byte OPCODE_GETQ = 0x09;
    public static final byte OPCODE_NOOP = 0x0A;
    public static final byte OPCODE_VERSION = 0x0B;
    public static final byte OPCODE_GETK = 0x0C;
    public static final byte OPCODE_GETKQ = 0x0D;
    public static final byte OPCODE_APPEND = 0x0E;
    public static final byte OPCODE_PREPEND = 0x0F;
    public static final byte OPCODE_STAT = 0x10;
    public static final byte OPCODE_SETQ = 0x11;
    public static final byte OPCODE_ADDQ = 0x12;
    public static final byte OPCODE_REPLACEQ = 0x13;
    public static final byte OPCODE_DELETEQ = 0x14;
    public static final byte OPCODE_INCREMENTQ = 0x15;
    public static final byte OPCODE_DECREMENTQ = 0x16;
    public static final byte OPCODE_QUITQ = 0x17;
    public static final byte OPCODE_FLUSHQ = 0x18;
    public static final byte OPCODE_APPENDQ = 0x19;
    public static final byte OPCODE_PREPENDQ = 0x1A;

    // extra length
    public static final byte GET_EXTRA_LENGTH = 0;
    public static final byte SET_EXTRA_LENGTH = 8;

    public enum ResponseStatus {
        NO_ERROR(0x0000), KEY_NOT_FOUND(0x0001), KEY_EXISTS(0x0002), VALUE_TOO_LARGE(0x0003),
        INVALID_ARGUMENTS(0x0004), ITEM_NOT_STORED(0x0005),
        INCR_OR_DECR_ON_NON_NUMERIC_VALUE(0x0006), VBUCKET_WRONG(0x0007), AUTH_ERROR(0x0008),
        AUTH_CONTINUE(0x0009), UNKNOWN(0x0081), OUT_OF_MEMORY(0x0082), NOT_SUPPORTED(0x0083),
        INTERNAL_ERROR(0x0084), BUSY(0x0085), TEMP_FAILURE(0x0086);

        public final short status;

        private ResponseStatus(int status) {
            this.status = (short) status;
        }

        public short getStatus() {
            return status;
        }

        public String getMessage() {
            String name = super.name();
            return name.replace("_", " ").toLowerCase();
        }

    }

    private BinaryProtocol() {}

    public static String resolveName(final byte opcode) {
        switch (opcode) {
            case OPCODE_GET:
                return "GET";
            case OPCODE_SET:
                return "SET";
            case OPCODE_ADD:
                return "ADD";
            case OPCODE_REPLACE:
                return "REPLACE";
            case OPCODE_DELETE:
                return "DELETE";
            case OPCODE_INCREMENT:
                return "INCREMENT";
            case OPCODE_DECREMENT:
                return "DECREMENT";
            case OPCODE_QUIT:
                return "QUIT";
            case OPCODE_FLUSH:
                return "FLUSH";
            case OPCODE_GETQ:
                return "GETQ";
            case OPCODE_NOOP:
                return "NOOP";
            case OPCODE_VERSION:
                return "VERSION";
            case OPCODE_GETK:
                return "GETK";
            case OPCODE_GETKQ:
                return "GETKQ";
            case OPCODE_APPEND:
                return "APPEND";
            case OPCODE_PREPEND:
                return "PREPEND";
            case OPCODE_STAT:
                return "STAT";
            case OPCODE_SETQ:
                return "SETQ";
            case OPCODE_ADDQ:
                return "ADDQ";
            case OPCODE_REPLACEQ:
                return "REPLACEQ";
            case OPCODE_DELETEQ:
                return "DELETEQ";
            case OPCODE_INCREMENTQ:
                return "INCREMENTQ";
            case OPCODE_DECREMENTQ:
                return "DECREMENTQ";
            case OPCODE_QUITQ:
                return "QUITQ";
            case OPCODE_FLUSHQ:
                return "FLUSHQ";
            case OPCODE_APPENDQ:
                return "APPENDQ";
            case OPCODE_PREPENDQ:
                return "PREPENDQ";
            default:
                return "Unknown opcode (" + opcode + ')';
        }
    }

    public static boolean noreply(final byte opcode) {
        switch (opcode) {
            case OPCODE_GETQ:
            case OPCODE_GETKQ:
            case OPCODE_SETQ:
            case OPCODE_ADDQ:
            case OPCODE_REPLACEQ:
            case OPCODE_DELETEQ:
            case OPCODE_INCREMENTQ:
            case OPCODE_DECREMENTQ:
            case OPCODE_QUITQ:
            case OPCODE_FLUSHQ:
            case OPCODE_APPENDQ:
            case OPCODE_PREPENDQ:
                return true;
            default:
                return false;
        }
    }

    public static boolean broadcast(final byte opcode) {
        switch (opcode) {
            case OPCODE_FLUSH:
            case OPCODE_NOOP:
            case OPCODE_STAT:
            case OPCODE_FLUSHQ:
                return true;
            default:
                return false;
        }
    }

    public static byte asSyncOp(final byte opcode) {
        switch (opcode) {
            case OPCODE_GETQ:
                return OPCODE_GET;
            case OPCODE_GETKQ:
                return OPCODE_GETK;
            case OPCODE_SETQ:
                return OPCODE_SET;
            case OPCODE_ADDQ:
                return OPCODE_ADD;
            case OPCODE_REPLACEQ:
                return OPCODE_REPLACE;
            case OPCODE_DELETEQ:
                return OPCODE_DELETE;
            case OPCODE_INCREMENTQ:
                return OPCODE_INCREMENT;
            case OPCODE_DECREMENTQ:
                return OPCODE_DECREMENT;
            case OPCODE_APPENDQ:
                return OPCODE_APPEND;
            case OPCODE_PREPENDQ:
                return OPCODE_PREPEND;
            default:
                return opcode;
        }
    }

    public static boolean surpressSuccessResponse(final byte opcode) {
        switch (opcode) {
            case OPCODE_SETQ:
            case OPCODE_ADDQ:
            case OPCODE_REPLACEQ:
            case OPCODE_DELETEQ:
            case OPCODE_INCREMENTQ:
            case OPCODE_DECREMENTQ:
            case OPCODE_APPENDQ:
            case OPCODE_PREPENDQ:
                return true;
            default:
                return false;
        }
    }

    public static final class Packet {

        @Nonnull
        final Header header;
        @Nullable
        final ChannelBuffer body;

        public Packet(@Nonnull Header header, @Nullable ChannelBuffer body) {
            this.header = header;
            this.body = body;
        }

        public Header getHeader() {
            return header;
        }

        public ChannelBuffer getBody() {
            return body;
        }

    }

    public static final class Header {

        byte magic; // 0        Magic number.
        byte opcode; // 1        Command code.
        short keyLength = 0; // 2,3      Length in bytes of the text key that follows the command extras.
        byte extraLength = 0; // 4        Length in bytes of the command extras.
        byte dataType = 0; // 5        Reserved for future use
        short status = 0; // 6,7      Status of the response.
        int totalBody = 0; // 8-11     Length in bytes of extra + key + value.
        int opaque = 0; // 12-15    Will be copied back to you in the response.
        long cas = 0L; // 16-23    Data version check.

        public Header() {}

        public Header(Header requestHeader) {
            this.magic = MAGIC_BYTE_RESPONSE;
            this.opcode = requestHeader.opcode;
            this.opaque = requestHeader.opaque;
        }

        public Header(byte magic, byte opcode) {
            this.magic = magic;
            this.opcode = opcode;
        }

        public byte getMagic() {
            return magic;
        }

        public byte getOpcode() {
            return opcode;
        }

        public short getKeyLength() {
            return keyLength;
        }

        public byte getExtraLength() {
            return extraLength;
        }

        public byte getDataType() {
            return dataType;
        }

        public short getStatus() {
            return status;
        }

        public int getTotalBody() {
            return totalBody;
        }

        public int getOpaque() {
            return opaque;
        }

        public long getCas() {
            return cas;
        }

        public void setBodyLength(byte extraLength, int keyLength, int valueLength) {
            assert (keyLength <= Short.MAX_VALUE);
            this.keyLength = (short) keyLength;
            this.extraLength = extraLength;
            this.totalBody = valueLength + extraLength + keyLength;
        }

        public Header magic(byte magic) {
            this.magic = magic;
            return this;
        }

        public Header opcode(byte opcode) {
            this.opcode = opcode;
            return this;
        }

        public Header keyLength(short keyLength) {
            this.keyLength = keyLength;
            return this;
        }

        public Header extraLength(byte extraLength) {
            this.extraLength = extraLength;
            return this;
        }

        public Header dataType(byte dataType) {
            this.dataType = dataType;
            return this;
        }

        public Header status(short status) {
            this.status = status;
            return this;
        }

        public Header totalBody(int totalBody) {
            this.totalBody = totalBody;
            return this;
        }

        public Header opaque(int opaque) {
            this.opaque = opaque;
            return this;
        }

        public Header cas(long cas) {
            this.cas = cas;
            return this;
        }

        public void decode(final ChannelBuffer buffer) {
            magic = buffer.readByte();
            opcode = buffer.readByte();
            keyLength = buffer.readShort();
            extraLength = buffer.readByte();
            dataType = buffer.readByte();
            status = buffer.readShort();
            totalBody = buffer.readInt();
            opaque = buffer.readInt();
            cas = buffer.readLong();
        }

        public void encode(final ChannelBuffer buffer) {
            buffer.writeByte(magic);
            buffer.writeByte(opcode);
            buffer.writeShort(keyLength);
            buffer.writeByte(extraLength);
            buffer.writeByte(dataType);
            buffer.writeShort(status);
            buffer.writeInt(totalBody);
            buffer.writeInt(opaque);
            buffer.writeLong(cas);
        }

        public void decode(final ByteBuffer buffer) {
            magic = buffer.get();
            opcode = buffer.get();
            keyLength = buffer.getShort();
            extraLength = buffer.get();
            dataType = buffer.get();
            status = buffer.getShort();
            totalBody = buffer.getInt();
            opaque = buffer.getInt();
            cas = buffer.getLong();
        }

        public void encode(final ByteBuffer buffer) {
            buffer.put(magic);
            buffer.put(opcode);
            buffer.putShort(keyLength);
            buffer.put(extraLength);
            buffer.put(dataType);
            buffer.putShort(status);
            buffer.putInt(totalBody);
            buffer.putInt(opaque);
            buffer.putLong(cas);
        }

        @Override
        public String toString() {
            return "Header [cas=" + cas + ", dataType=" + dataType + ", extraLength=" + extraLength
                    + ", keyLength=" + keyLength + ", magic=" + magic + ", opaque=" + opaque
                    + ", opcode=" + opcode + ", status=" + status + ", totalBody=" + totalBody
                    + "]";
        }
    }

}
