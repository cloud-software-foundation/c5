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
package c5db;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by posix4e on 3/4/14.
 */
public class ByteBufferBackedInputStream extends InputStream {

  ByteBuffer buf;

  public ByteBufferBackedInputStream(ByteBuffer buf) {
    this.buf = buf;
  }

  public synchronized int read() throws IOException {
    if (!buf.hasRemaining()) {
      return -1;
    }
    return buf.get();
  }

  public synchronized int read(byte[] bytes, int off, int len) throws IOException {
    len = Math.min(len, buf.remaining());
    buf.get(bytes, off, len);
    return len;
  }
}
