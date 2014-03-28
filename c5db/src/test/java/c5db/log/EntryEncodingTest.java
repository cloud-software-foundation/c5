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
import com.dyuproject.protostuff.Schema;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import static c5db.log.EntryEncoding.decodeAndCheckCrc;
import static c5db.log.EntryEncoding.encodeDelimitedWithCrc;
import static org.junit.Assert.assertEquals;

public class EntryEncodingTest {
  private static Schema<OLogImmutableHeader> SCHEMA = OLogImmutableHeader.getSchema();

  private static void assertSameMessage(OLogImmutableHeader messageA, OLogImmutableHeader messageB) {
    assertEquals(messageA.getSeqNum(), messageB.getSeqNum());
    assertEquals(messageA.getTerm(), messageB.getTerm());
    assertEquals(messageA.getQuorumId(), messageB.getQuorumId());
    assertEquals(messageA.getRemainingLength(), messageB.getRemainingLength());
  }

  private static void testEncodeThenDecodeAndCheckCrc(final OLogImmutableHeader entryToTest) throws IOException {
    final PipedOutputStream pipedOutputStream = new PipedOutputStream();
    final InputStream readFromMe = new PipedInputStream(pipedOutputStream);
    final WritableByteChannel writeToMe = Channels.newChannel(pipedOutputStream);

    final List<ByteBuffer> serialized = encodeDelimitedWithCrc(SCHEMA, entryToTest);
    for (ByteBuffer buffer : serialized) {
      writeToMe.write(buffer);
    }

    final OLogImmutableHeader result = decodeAndCheckCrc(readFromMe, SCHEMA);

    assertSameMessage(entryToTest, result);
  }

  @Test
  public void testThatEncodeAndDecodeAreInverses() throws Exception {
    testEncodeThenDecodeAndCheckCrc(
        new OLogImmutableHeader("quorumId", Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE));
    testEncodeThenDecodeAndCheckCrc(
        new OLogImmutableHeader(
            "tableName,\\x00,0.48445111e3a0dc70a14610c61b025d60.", Long.MIN_VALUE, Long.MIN_VALUE, Integer.MIN_VALUE));
  }
}
