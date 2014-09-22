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
