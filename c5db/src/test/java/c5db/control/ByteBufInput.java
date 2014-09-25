/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.control;

import io.netty.buffer.ByteBuf;
import io.protostuff.ByteString;
import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.ProtobufException;
import io.protostuff.Schema;
import io.protostuff.StringSerializer.STRING;
import io.protostuff.UninitializedMessageException;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.protostuff.WireFormat.WIRETYPE_END_GROUP;
import static io.protostuff.WireFormat.WIRETYPE_FIXED32;
import static io.protostuff.WireFormat.WIRETYPE_FIXED64;
import static io.protostuff.WireFormat.WIRETYPE_LENGTH_DELIMITED;
import static io.protostuff.WireFormat.WIRETYPE_START_GROUP;
import static io.protostuff.WireFormat.WIRETYPE_TAIL_DELIMITER;
import static io.protostuff.WireFormat.WIRETYPE_VARINT;
import static io.protostuff.WireFormat.getTagFieldNumber;
import static io.protostuff.WireFormat.getTagWireType;
import static io.protostuff.WireFormat.makeTag;

public final class ByteBufInput implements Input {
  static ProtobufException misreportedSize() {
    return new ProtobufException(
        "CodedInput encountered an embedded string or bytes " +
            "that misreported its size.");
  }

  static ProtobufException negativeSize() {
    return new ProtobufException(
        "CodedInput encountered an embedded string or message " +
            "which claimed to have negative size.");
  }

  static ProtobufException malformedVarint() {
    return new ProtobufException(
        "CodedInput encountered a malformed varint.");
  }

  static ProtobufException invalidTag() {
    return new ProtobufException(
        "Protocol message contained an invalid tag (zero).");
  }

  static ProtobufException invalidEndTag() {
    return new ProtobufException(
        "Protocol message end-group tag did not match expected tag.");
  }

  static ProtobufException invalidWireType() {
    return new ProtobufException(
        "Protocol message tag had invalid wire type.");
  }


  static final int TAG_TYPE_BITS = 3;
  static final int TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1;


  private final ByteBuf buffer;
  // private final byte[] buffer;
  private int lastTag = 0;
  // private int offset, limit, lastTag = 0;

  /**
   * If true, the nested messages are group-encoded
   */
  public final boolean decodeNestedMessageAsGroup;

  /**
   * An input for a ByteBuffer
   *
   * @param buffer            the buffer to read from, it will be sliced
   * @param protostuffMessage if we are parsing a protostuff (true) or protobuf (false) message
   */
  public ByteBufInput(ByteBuf buffer, boolean protostuffMessage) {
    this.buffer = buffer.slice();
    this.buffer.markReaderIndex();
    this.decodeNestedMessageAsGroup = protostuffMessage;
  }

  /**
   * Resets the offset and the limit of the internal buffer.
   */
  public ByteBufInput reset(int offset, int len) {
    buffer.resetReaderIndex();

    return this;
  }

  /**
   * Returns the current offset (the position).
   */
  public int currentOffset() {
    return buffer.readerIndex();
  }

  public int limit() {
    return buffer.writerIndex();
  }

  /**
   * Returns the last tag.
   */
  public int getLastTag() {
    return lastTag;
  }

  /**
   * Attempt to read a field tag, returning zero if we have reached EOF. Protocol message parsers use this to read
   * tags, since a protocol message may legally end wherever a tag occurs, and zero is not a valid tag number.
   */
  public int readTag() throws IOException {
    if (!buffer.isReadable()) {
      lastTag = 0;
      return 0;
    }

    final int tag = readRawVarint32();
    if (tag >>> TAG_TYPE_BITS == 0) {
      // If we actually read zero, that's not a valid tag.
      throw invalidTag();
    }
    lastTag = tag;
    return tag;
  }

  /**
   * Verifies that the last call to readTag() returned the given tag value. This is used to verify that a nested group
   * ended with the correct end tag.
   *
   * @throws ProtobufException {@code value} does not match the last tag.
   */
  public void checkLastTagWas(final int value) throws ProtobufException {
    if (lastTag != value) {
      throw invalidEndTag();
    }
  }

  /**
   * Reads and discards a single field, given its tag value.
   *
   * @return {@code false} if the tag is an endgroup tag, in which case nothing is skipped. Otherwise, returns
   * {@code true}.
   */
  public boolean skipField(final int tag) throws IOException {
    switch (getTagWireType(tag)) {
      case WIRETYPE_VARINT:
        readInt32();
        return true;
      case WIRETYPE_FIXED64:
        readRawLittleEndian64();
        return true;
      case WIRETYPE_LENGTH_DELIMITED:
        final int size = readRawVarint32();
        if (size < 0) {
          throw negativeSize();
        }
        buffer.skipBytes(size);
        // offset += size;
        return true;
      case WIRETYPE_START_GROUP:
        skipMessage();
        checkLastTagWas(makeTag(getTagFieldNumber(tag), WIRETYPE_END_GROUP));
        return true;
      case WIRETYPE_END_GROUP:
        return false;
      case WIRETYPE_FIXED32:
        readRawLittleEndian32();
        return true;
      default:
        throw invalidWireType();
    }
  }

  /**
   * Reads and discards an entire message. This will read either until EOF or until an endgroup tag, whichever comes
   * first.
   */
  public void skipMessage() throws IOException {
    while (true) {
      final int tag = readTag();
      if (tag == 0 || !skipField(tag)) {
        return;
      }
    }
  }

  public <T> void handleUnknownField(int fieldNumber, Schema<T> schema) throws IOException {
    skipField(lastTag);
  }

  public <T> int readFieldNumber(Schema<T> schema) throws IOException {
    if (!buffer.isReadable()) {
      lastTag = 0;
      return 0;
    }

    final int tag = readRawVarint32();
    final int fieldNumber = tag >>> TAG_TYPE_BITS;
    if (fieldNumber == 0) {
      if (decodeNestedMessageAsGroup &&
          WIRETYPE_TAIL_DELIMITER == (tag & TAG_TYPE_MASK)) {
        // protostuff's tail delimiter for streaming
        // 2 options: length-delimited or tail-delimited.
        lastTag = 0;
        return 0;
      }
      // If we actually read zero, that's not a valid tag.
      throw invalidTag();
    }
    if (decodeNestedMessageAsGroup && WIRETYPE_END_GROUP == (tag & TAG_TYPE_MASK)) {
      lastTag = 0;
      return 0;
    }

    lastTag = tag;
    return fieldNumber;
  }

  /**
   * Read a {@code double} field value from the internal buffer.
   */
  public double readDouble() throws IOException {
    return Double.longBitsToDouble(readRawLittleEndian64());
  }

  /**
   * Read a {@code float} field value from the internal buffer.
   */
  public float readFloat() throws IOException {
    return Float.intBitsToFloat(readRawLittleEndian32());
  }

  /**
   * Read a {@code uint64} field value from the internal buffer.
   */
  public long readUInt64() throws IOException {
    return readRawVarint64();
  }

  /**
   * Read an {@code int64} field value from the internal buffer.
   */
  public long readInt64() throws IOException {
    return readRawVarint64();
  }

  /**
   * Read an {@code int32} field value from the internal buffer.
   */
  public int readInt32() throws IOException {
    return readRawVarint32();
  }

  /**
   * Read a {@code fixed64} field value from the internal buffer.
   */
  public long readFixed64() throws IOException {
    return readRawLittleEndian64();
  }

  /**
   * Read a {@code fixed32} field value from the internal buffer.
   */
  public int readFixed32() throws IOException {
    return readRawLittleEndian32();
  }

  /**
   * Read a {@code bool} field value from the internal buffer.
   */
  public boolean readBool() throws IOException {
    return buffer.readByte() != 0;
  }

  /**
   * Read a {@code uint32} field value from the internal buffer.
   */
  public int readUInt32() throws IOException {
    return readRawVarint32();
  }

  /**
   * Read an enum field value from the internal buffer. Caller is responsible for converting the numeric value to an
   * actual enum.
   */
  public int readEnum() throws IOException {
    return readRawVarint32();
  }

  /**
   * Read an {@code sfixed32} field value from the internal buffer.
   */
  public int readSFixed32() throws IOException {
    return readRawLittleEndian32();
  }

  /**
   * Read an {@code sfixed64} field value from the internal buffer.
   */
  public long readSFixed64() throws IOException {
    return readRawLittleEndian64();
  }

  /**
   * Read an {@code sint32} field value from the internal buffer.
   */
  public int readSInt32() throws IOException {
    final int n = readRawVarint32();
    return (n >>> 1) ^ -(n & 1);
  }

  /**
   * Read an {@code sint64} field value from the internal buffer.
   */
  public long readSInt64() throws IOException {
    final long n = readRawVarint64();
    return (n >>> 1) ^ -(n & 1);
  }

  public String readString() throws IOException {
    final int length = readRawVarint32();
    if (length < 0) {
      throw negativeSize();
    }

    if (buffer.readableBytes() < length) {
      throw misreportedSize();
    }

    if (buffer.hasArray()) {
      final int currPosition = buffer.readerIndex();

      buffer.skipBytes(length);

      return STRING.deser(buffer.array(),
          buffer.arrayOffset() + currPosition,
          length);
    } else {
      byte[] tmp = new byte[length];
      buffer.readBytes(tmp);
      return STRING.deser(tmp);
    }
  }

  public ByteString readBytes() throws IOException {
    return ByteString.copyFrom(readByteArray());
//    return ByteString.wrap(readByteArray());
  }

  public byte[] readByteArray() throws IOException {
    final int length = readRawVarint32();
    if (length < 0) {
      throw negativeSize();
    }

    if (!buffer.isReadable(length))
    // if(offset + length > limit)
    {
      throw misreportedSize();
    }

    final byte[] copy = new byte[length];
    buffer.readBytes(copy);
    return copy;
  }

  public <T> T mergeObject(T value, final Schema<T> schema) throws IOException {
    if (decodeNestedMessageAsGroup) {
      return mergeObjectEncodedAsGroup(value, schema);
    }

    final int length = readRawVarint32();
    if (length < 0) {
      throw negativeSize();
    }

    if (!buffer.isReadable(length)) {
      throw misreportedSize();
    }

    ByteBuf dup = buffer.slice(buffer.readerIndex(), length);

    if (value == null) {
      value = schema.newMessage();
    }
    ByteBufInput nestedInput = new ByteBufInput(dup, decodeNestedMessageAsGroup);
    schema.mergeFrom(nestedInput, value);
    if (!schema.isInitialized(value)) {
      throw new UninitializedMessageException(value, schema);
    }
    nestedInput.checkLastTagWas(0);

    buffer.skipBytes(length);
    return value;
  }

  private <T> T mergeObjectEncodedAsGroup(T value, final Schema<T> schema) throws IOException {
    if (value == null) {
      value = schema.newMessage();
    }
    schema.mergeFrom(this, value);
    if (!schema.isInitialized(value)) {
      throw new UninitializedMessageException(value, schema);
    }
    // handling is in #readFieldNumber
    checkLastTagWas(0);
    return value;
  }

  /**
   * Reads a var int 32 from the internal byte buffer.
   */
  public int readRawVarint32() throws IOException {
    byte tmp = buffer.readByte();
    if (tmp >= 0) {
      return tmp;
    }
    int result = tmp & 0x7f;
    if ((tmp = buffer.readByte()) >= 0) {
      result |= tmp << 7;
    } else {
      result |= (tmp & 0x7f) << 7;
      if ((tmp = buffer.readByte()) >= 0) {
        result |= tmp << 14;
      } else {
        result |= (tmp & 0x7f) << 14;
        if ((tmp = buffer.readByte()) >= 0) {
          result |= tmp << 21;
        } else {
          result |= (tmp & 0x7f) << 21;
          result |= (tmp = buffer.readByte()) << 28;
          if (tmp < 0) {
            // Discard upper 32 bits.
            for (int i = 0; i < 5; i++) {
              if (buffer.readByte() >= 0) {
                return result;
              }
            }
            throw malformedVarint();
          }
        }
      }
    }
    return result;
  }

  /**
   * Reads a var int 64 from the internal byte buffer.
   */
  public long readRawVarint64() throws IOException {
    int shift = 0;
    long result = 0;
    while (shift < 64) {
      final byte b = buffer.readByte();
      result |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return result;
      }
      shift += 7;
    }
    throw malformedVarint();
  }

  /**
   * Read a 32-bit little-endian integer from the internal buffer.
   */
  public int readRawLittleEndian32() throws IOException {
    final byte[] bs = new byte[4];
    buffer.readBytes(bs);

    return (((int) bs[0] & 0xff)) |
        (((int) bs[1] & 0xff) << 8) |
        (((int) bs[2] & 0xff) << 16) |
        (((int) bs[3] & 0xff) << 24);
  }

  /**
   * Read a 64-bit little-endian integer from the internal byte buffer.
   */
  public long readRawLittleEndian64() throws IOException {
    final byte[] bs = new byte[8];
    buffer.readBytes(bs);

    return (((long) bs[0] & 0xff)) |
        (((long) bs[1] & 0xff) << 8) |
        (((long) bs[2] & 0xff) << 16) |
        (((long) bs[3] & 0xff) << 24) |
        (((long) bs[4] & 0xff) << 32) |
        (((long) bs[5] & 0xff) << 40) |
        (((long) bs[6] & 0xff) << 48) |
        (((long) bs[7] & 0xff) << 56);
  }

  public void transferByteRangeTo(Output output, boolean utf8String, int fieldNumber,
                                  boolean repeated) throws IOException {
    final int length = readRawVarint32();
    if (length < 0) {
      throw negativeSize();
    }

    if (utf8String) {
      // if it is a UTF string, we have to call the writeByteRange.

      if (buffer.hasArray()) {
        output.writeByteRange(true, fieldNumber, buffer.array(),
            buffer.arrayOffset() + buffer.readerIndex(), length, repeated);
        buffer.skipBytes(length);
      } else {
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        output.writeByteRange(true, fieldNumber, bytes, 0, bytes.length, repeated);
      }
    } else {
      // Do the potentially vastly more efficient potential splice call.
      if (!buffer.isReadable(length)) {
        throw misreportedSize();
      }

      ByteBuf dup = buffer.slice(buffer.readerIndex(), length);
      output.writeBytes(fieldNumber, dup.nioBuffer(), repeated);

      buffer.skipBytes(length);
    }
  }

  /**
   * Reads a byte array/ByteBuffer value.
   */
  public ByteBuffer readByteBuffer() throws IOException {
    return ByteBuffer.wrap(readByteArray());
  }
}
