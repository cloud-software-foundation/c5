/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package c5db.log;

import c5db.generated.OLogImmutableHeader;
import c5db.generated.OLogMutableHeader;
import c5db.replication.generated.LogEntry;
import c5db.util.CrcInputStream;
import com.dyuproject.protostuff.LinkBuffer;
import com.dyuproject.protostuff.LowCopyProtobufOutput;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.Adler32;

import static c5db.generated.OLogMutableHeader.Type.DATA;
import static c5db.generated.OLogMutableHeader.Type.TRUNCATION;

/**
 * Contains methods used for encoding and decoding WAL entries
 */
public class EntryEncoding {

  /**
   * Exception class for use when reading a bad CRC from disk
   */
  public static class CrcError extends IllegalStateException {
    public CrcError(String s) {
      super(s);
    }
  }

  /**
   * Contains methods for use when iterating over entries in the log.
   */
  static class IterationInfo {
    final OLogImmutableHeader immutableHeader;
    final OLogMutableHeader mutableHeader;
    final long mutableHeaderFilePos;
    private final InputStream inputStream;

    IterationInfo(InputStream inputStream,
                  OLogImmutableHeader immutableHeader,
                  OLogMutableHeader mutableHeader,
                  long mutableHeaderFilePos) {
      this.inputStream = inputStream;
      this.immutableHeader = immutableHeader;
      this.mutableHeader = mutableHeader;
      this.mutableHeaderFilePos = mutableHeaderFilePos;
    }

    long getSeqNum() {
      return immutableHeader.getSeqNum();
    }

    long getTerm() {
      return immutableHeader.getTerm();
    }

    ByteBuffer getContent() throws IOException {
      final int contentLength = mutableHeader.getContentLength();
      assert mutableHeader.getType() == DATA;
      assert contentLength >= 0;
      if (contentLength == 0) {
        return ByteBuffer.allocate(0);
      } else {
        return getAndCheckContent(inputStream, contentLength);
      }
    }
  }

  /**
   * Return a ByteBuffer[], containing the serialized form of the passed log entry, ready to be written to disk. If
   * {@code content} is null, or if its remaining() is zero, neither the content nor the final CRC(s) will be included.
   *
   * @param entry    The entry to be written to disk. No position change will be performed on its ByteBuffer.
   * @param quorumId The quorum ID.
   * @return The array of ByteBuffers containing serialized data.
   */
  public static ByteBuffer[] encodeEntry(LogEntry entry, String quorumId) {
    // TODO Future optimization: replace quorum names with an ID

    try {
      final OLogMutableHeader mutableHeader = createMutableHeader(entry);
      final List<ByteBuffer> entryBufs = encodeDelimitedWithCrc(OLogMutableHeader.getSchema(), mutableHeader);

      if (mutableHeader.getContentLength() > 0) {
        entryBufs.addAll(writeContentWithCrc(entry.getData()));
      }

      int length = 0;
      for (ByteBuffer b : entryBufs) {
        length += b.remaining();
        // TODO catch overflow?
      }

      final OLogImmutableHeader immutableHeader = new OLogImmutableHeader(
          quorumId,
          entry.getIndex(),
          entry.getTerm(),
          length);
      final List<ByteBuffer> immutableHeaderBufs =
          encodeDelimitedWithCrc(OLogImmutableHeader.getSchema(), immutableHeader);

      return Iterables.toArray(
          Iterables.concat(immutableHeaderBufs, entryBufs),
          ByteBuffer.class);
    } catch (IOException e) {
      // This method performs no IO, so it should not actually be possible for an IOException to be thrown.
      // But just in case...
      throw new RuntimeException(e);
    }
  }

  /**
   * Return a ByteBuffer[] containing the serialized form of a truncation marker -- data that can be written
   * over an existing entry to logically delete it.
   *
   * @param skipForward The next valid file position after a series of truncated entries.
   * @return The serialized truncation marker
   */
  public static ByteBuffer[] encodeTruncationMarker(long skipForward) {
    final OLogMutableHeader truncationMarker = createTruncationMarker(skipForward);
    final List<ByteBuffer> bufs = encodeDelimitedWithCrc(OLogMutableHeader.getSchema(), truncationMarker);
    return Iterables.toArray(bufs, ByteBuffer.class);
  }

  /**
   * Serialize a protostuff message object, prefixed with message length, and suffixed with a 4-byte CRC.
   *
   * @param schema  Protostuff message schema
   * @param message Object to serialize
   * @param <T>     Message type
   * @return A list of ByteBuffers containing a varInt length, followed by the message, followed by a 4-byte CRC.
   */
  public static <T> List<ByteBuffer> encodeDelimitedWithCrc(Schema<T> schema, T message) {
    final Adler32 crc = new Adler32();
    final LinkBuffer messageBuf = new LinkBuffer();
    final LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput(messageBuf);

    try {
      schema.writeTo(lcpo, message);

      final int length = (int) lcpo.buffer.size();
      final LinkBuffer lengthBuf = new LinkBuffer().writeVarInt32(length);

      lengthBuf.finish().forEach(crc::update);
      messageBuf.finish().forEach(crc::update);
      final LinkBuffer crcBuf = new LinkBuffer(8);
      putCrc(crcBuf, crc.getValue());

      return Lists.newArrayList(
          Iterables.concat(lengthBuf.getBuffers(), messageBuf.getBuffers(), crcBuf.finish()));
    } catch (IOException e) {
      // This method performs no IO, so it should not actually be possible for an IOException to be thrown.
      // But just in case...
      throw new RuntimeException(e);
    }
  }

  /**
   * Decode a message from the passed input stream, and compute and verify its CRC. This method reads
   * data written by the method {@link EntryEncoding#encodeDelimitedWithCrc}.
   *
   * @param inputStream Input stream, opened for reading and positioned just before the length-prepended header
   * @return The deserialized, constructed, validated message
   * @throws IOException                     if a problem is encountered while reading or parsing
   * @throws c5db.log.EntryEncoding.CrcError if the recorded CRC of the message does not match its computed CRC.
   */
  public static <T> T decodeAndCheckCrc(InputStream inputStream, Schema<T> schema)
      throws IOException, CrcError {
    // TODO this should check the length first and compare it with a passed-in maximum length
    final T message = schema.newMessage();
    final CrcInputStream crcStream = new CrcInputStream(inputStream, new Adler32());
    ProtostuffIOUtil.mergeDelimitedFrom(crcStream, message, schema);

    final long computedCrc = crcStream.getValue();
    final long diskCrc = readCrc(inputStream);
    if (diskCrc != computedCrc) {
      throw new CrcError("CRC mismatch on deserialized message " + message.toString());
    }

    return message;
  }

  /**
   * Create a truncation marker, used to overwrite an existing mutable header.
   */
  private static OLogMutableHeader createTruncationMarker(long skipForward) {
    return new OLogMutableHeader(TRUNCATION, 0, skipForward);
  }

  /**
   * Create an OLogMutableHeader, ready to be written to disk.
   *
   * @param entry Entry to be used. The data field of this entry is only used for its length, and the
   *              length is calculated without performing any position change on the ByteBuffer.
   * @return A new OLogEntryHeader.
   */
  private static OLogMutableHeader createMutableHeader(LogEntry entry) {
    final int contentLength;
    if (entry.getData() == null) {
      contentLength = 0;
    } else {
      contentLength = entry.getData().remaining();
    }

    return new OLogMutableHeader(DATA, contentLength, 0);
  }

  /**
   * Write the passed data to buffers, followed by one or more 4-byte CRCs.
   *
   * @param content non-null ByteBuffer; no reset will be performed on it.
   * @return list of ByteBuffers
   */
  private static List<ByteBuffer> writeContentWithCrc(ByteBuffer content) throws IOException {
    assert content != null;

    final LinkBuffer contentBuf = new LinkBuffer();
    final Adler32 crc = new Adler32();
    contentBuf.writeByteBuffer(content);
    crc.update(content);
    putCrc(contentBuf, crc.getValue());
    return contentBuf.finish();
  }

  /**
   * Write a passed CRC to the passed buffer. The CRC is a 4-byte unsigned integer stored in a long; write it
   * as (fixed length) 4 bytes.
   *
   * @param writeTo Buffer to write to; exactly 4 bytes will be written.
   * @param crc     CRC to write; caller guarantees that the code is within the range:
   *                0 <= CRC < 2^32
   */
  private static void putCrc(final LinkBuffer writeTo, final long crc) throws IOException {
    // To store the CRC in an int, we need to subtract to convert it from unsigned to signed.
    final long shiftedCrc = crc + Integer.MIN_VALUE;
    writeTo.writeInt32(Ints.checkedCast(shiftedCrc));
  }

  private static long readCrc(InputStream inputStream) throws IOException {
    int shiftedCrc = (new DataInputStream(inputStream)).readInt();
    return ((long) shiftedCrc) - Integer.MIN_VALUE;
  }

  /**
   * Read a specified number of bytes from the input stream (the "content"), then read one or more CRC codes and
   * check the validity of the data.
   *
   * @param inputStream   Input stream, opened for reading and positioned just before the content
   * @param contentLength Length of data to read from inputStream, not including any trailing CRCs
   * @return The read content, as a ByteBuffer.
   * @throws IOException
   */
  private static ByteBuffer getAndCheckContent(InputStream inputStream, int contentLength)
      throws IOException, CrcError {
    // TODO probably not the correct way to do this... should use IOUtils?
    final CrcInputStream crcStream = new CrcInputStream(inputStream, new Adler32());
    final byte[] content = new byte[contentLength];
    final int len = crcStream.read(content);

    if (len < contentLength) {
      // Data wasn't available that we expected to be
      throw new IllegalStateException("Reading a log entry's contents returned fewer than expected bytes");
    }

    final long computedCrc = crcStream.getValue();
    final long diskCrc = readCrc(inputStream);
    if (diskCrc != computedCrc) {
      throw new CrcError("CRC mismatch on log entry contents");
    }

    return ByteBuffer.wrap(content);
  }
}
