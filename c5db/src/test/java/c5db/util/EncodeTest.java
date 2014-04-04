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
package c5db.util;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.zookeeper.server.ByteBufferInputStream;
import org.junit.Test;

import java.nio.ByteBuffer;

public class EncodeTest {

  @Test
  public void testVarInt() throws Exception {
    //byte[] thingy = new byte[50];

    long[] lengths = {1, 20, 200, 1024, 2048, 4000, 10000, 50000,
        100000, 1024 * 1024,
        ((long) Integer.MAX_VALUE) * 100,
        -1, -200, -5000};


    for (long value : lengths) {
      // do the test:
      ByteBufferOutputStream bbos = new ByteBufferOutputStream(12);
      CodedOutputStream cos = CodedOutputStream.newInstance(bbos);
      long newvalue = (value << 4) | 8;
      //cos.writeRawVarint64(newvalue);
      cos.writeSInt64NoTag(newvalue);
      cos.flush();

      ByteBuffer bb = bbos.getByteBuffer();
      System.out.println("value: " + value + ", length: " + bb.remaining());

      ByteBufferInputStream bbis = new ByteBufferInputStream(bb);
      CodedInputStream cis = CodedInputStream.newInstance(bbis);
      long outval = cis.readSInt64();
      long actual = outval >> 4;
      long tag = outval & 0x0F;
      System.out.println("  transformed we are: " + outval + " actual: " + actual + " tag: " + tag);
    }
  }

}
