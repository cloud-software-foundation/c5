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

import c5db.C5CommonTestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class LogFileManagerTest {
  private static Path testDirectory = (new C5CommonTestUtil()).getDataTestDir("log-file-manager");
  private LogFileManager logFileManager;

  private static void writeInts(LogFileManager.Writer writer, int start, int end) throws Exception {
    for (int i = start; i < end; i++) {
      ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
      buffer.asIntBuffer().put(i);
      writer.write(new ByteBuffer[]{buffer});
    }
  }

  private static void assertReadInts(DataInputStream dataInputStream, int start, int end) throws Exception {
    for (int i = start; i < end; i++) {
      assertEquals(i, dataInputStream.readInt());
    }
  }

  private static void assertIsAtEOF(InputStream inputStream) throws Exception {
    try {
      assertEquals(-1, inputStream.read());
    } catch (EOFException ignore) {
      // pass
    }
  }

  private static DataInputStream getDataInputStream(LogFileManager logFileManager) throws Exception {
    return new DataInputStream(Channels.newInputStream(logFileManager.getNewInputChannel()));
  }

  @Before
  public void setUp() throws Exception {
    logFileManager = new LogFileManager(testDirectory);
    logFileManager.moveLogsToArchive();
  }

  @After
  public void tearDown() throws Exception {
    logFileManager.moveLogsToArchive();
  }


  @Test
  public void testWritingAndThenReadingFromSameFile() throws Exception {
    try (
        LogFileManager.Writer writer = logFileManager.getNewWriter();
        DataInputStream dataInputStream1 = getDataInputStream(logFileManager);
        DataInputStream dataInputStream2 = getDataInputStream(logFileManager)
    ) {
      writeInts(writer, 0, 100);
      assertReadInts(dataInputStream1, 0, 100);

      writeInts(writer, 100, 200);
      assertReadInts(dataInputStream1, 100, 200);
      assertReadInts(dataInputStream2, 0, 200);

      assertIsAtEOF(dataInputStream1);
      assertIsAtEOF(dataInputStream2);
    }
  }

  @Test
  public void testArchivingLogAndStartingNewLog() throws Exception {
    try (
        LogFileManager.Writer writer = logFileManager.getNewWriter();
        DataInputStream dataInputStream = getDataInputStream(logFileManager)
    ) {
      writeInts(writer, 0, 100);
      assertReadInts(dataInputStream, 0, 100);
      assertIsAtEOF(dataInputStream);
    }

    logFileManager.roll();

    try (
        DataInputStream dataInputStream = getDataInputStream(logFileManager)
    ) {
      assertIsAtEOF(dataInputStream);
    }
  }

  @Test
  public void testFindAndUseExistingLog() throws Exception {
    try (
        LogFileManager.Writer writer = logFileManager.getNewWriter()
    ) {
      writeInts(writer, 0, 100);
    }

    LogFileManager newLogFileManager = new LogFileManager(testDirectory);

    try (
        DataInputStream dataInputStream = getDataInputStream(newLogFileManager)
    ) {
      assertReadInts(dataInputStream, 0, 100);
      assertIsAtEOF(dataInputStream);
    }
  }
}
