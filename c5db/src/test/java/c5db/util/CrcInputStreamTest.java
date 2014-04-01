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

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

import static org.junit.Assert.assertEquals;

public class CrcInputStreamTest {

  @Test
  public void testDirectlyCheckingCrc() throws Exception {
    final byte[] b = "The quick brown fox jumps over the \000\001\002\003".getBytes(Charset.forName("UTF-8"));
    final int len = b.length;

    // naively compute CRC
    Checksum crc = new Adler32();
    crc.update(b, 0, len);
    final long expectedCrc = crc.getValue();

    final InputStream inputStream = new ByteArrayInputStream(b);
    final CrcInputStream crcInputStream = new CrcInputStream(inputStream, new Adler32());

    // Read 4 individual bytes, then do byte array reads until the end of the input.
    for (int i = 0; i < 4; i++) {
      //noinspection ResultOfMethodCallIgnored
      crcInputStream.read();
    }

    int bytesRead;
    final byte[] buffer = new byte[3];
    do {
      bytesRead = crcInputStream.read(buffer);
    } while (bytesRead > 0);

    assertEquals(expectedCrc, crcInputStream.getValue());
  }
}
