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

import c5db.C5ServerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SYNC;
import static java.nio.file.StandardOpenOption.WRITE;

public class LogFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(LogFileManager.class);

  private final Path walDir;
  private final Path archiveDir;

  private File logFile = null;

  // TODO should this be in its own file?
  public static class Writer implements AutoCloseable {
    final FileChannel fileChannel;

    protected Writer(File logFile) throws IOException {
      fileChannel = FileChannel.open(logFile.toPath(), CREATE, APPEND);
    }

    @Override
    public void close() throws IOException {
      fileChannel.close();
    }

    public long position() throws IOException {
      return fileChannel.position();
    }

    public long write(ByteBuffer[] buffers) throws IOException {
      return fileChannel.write(buffers);
    }

    public void sync() throws IOException {
      fileChannel.force(true);
    }
  }

  public LogFileManager(Path basePath) throws IOException {
    this.walDir = basePath.resolve(C5ServerConstants.WAL_DIR);
    this.archiveDir = basePath.resolve(C5ServerConstants.ARCHIVE_DIR);

    createDirectoryStructure();
    createLogFileOrFindExisting();
  }

  protected File getLogFile() {
    return logFile;
  }

  /**
   * Initialize this LogFileManager instance by finding an existing write-ahead log file to use, or creating
   * a new one if an old one isn't found.
   *
   * @throws IOException If there was a problem creating a new file.
   */
  private void createLogFileOrFindExisting() throws IOException {
    this.logFile = findExistingLogFile();
    if (this.logFile == null) {
      this.logFile = createNewLogFile();
    }
  }

  private void createDirectoryStructure() throws IOException {
    createDirsIfNecessary(walDir);
    createDirsIfNecessary(archiveDir);
  }

  /**
   * Create the directory at dirPath if it does not already exist.
   *
   * @param dirPath Path to directory to create.
   * @throws IOException
   */
  private void createDirsIfNecessary(Path dirPath) throws IOException {
    if (!dirPath.toFile().exists()) {
      boolean success = dirPath.toFile().mkdirs();
      if (!success) {
        LOG.error("Creation of path {} failed", dirPath);
        throw new IOException("Unable to setup log Path");
      }
    }
  }

  private File findExistingLogFile() {
    for (File file : allFilesInDirectory(walDir)) {
      if (file.getName().startsWith(C5ServerConstants.LOG_NAME)) {
        LOG.info("Existing WAL found {} ; using it", file);
        return file;
      }
    }
    return null;
  }

  private File[] allFilesInDirectory(Path dirPath) {
    File[] files = dirPath.toFile().listFiles();
    if (files == null) {
      return new File[]{};
    } else {
      return files;
    }
  }

  private File createNewLogFile() throws IOException {
    long fileNum = System.currentTimeMillis();
    File newFile = walDir.resolve(C5ServerConstants.LOG_NAME + fileNum).toFile();

    // TODO should we create now so that the object is in a state where a read will succeed immediately, or
    // TODO should we wait until some other call asks to create the file?
    boolean success = newFile.createNewFile();
    if (!success) {
      LOG.error("Creation of new log file failed: {}", newFile);
      throw new IOException("Creation of log file failed");
    }

    return newFile;
  }

  public Writer getNewWriter() throws IOException {
    return new Writer(logFile);
  }

  public FileChannel getNewInputChannel() throws IOException {
    return FileChannel.open(logFile.toPath(), READ);
  }

  public FileChannel getNewIOChannel() throws IOException {
    return FileChannel.open(logFile.toPath(), READ, WRITE, SYNC);
  }

  /**
   * Move the current log file(s) to the archive directory, and prepare to use a new log file.
   *
   * @throws java.io.IOException
   */
  public void roll() throws IOException {
    moveLogsToArchive();
    this.logFile = createNewLogFile();
  }

  /**
   * Move everything in the log directory to the archive directory.
   *
   * @throws IOException
   */
  void moveLogsToArchive() throws IOException {
    for (File file : allFilesInDirectory(walDir)) {
      boolean success = file.renameTo(archiveDir
          .resolve(file.getName())
          .toFile());
      if (!success) {
        String err = "Unable to move: " + file.getAbsolutePath() + " to " + walDir;
        throw new IOException(err);
      }
    }
  }

  /**
   * Clean out old logs.
   *
   * @param timestamp Only clear logs older than timestamp. Or if 0 then remove all logs.
   * @throws IOException
   */
  public void clearOldArchivedLogs(long timestamp) throws IOException {
    for (File file : allFilesInDirectory(archiveDir)) {
      if (timestamp == 0 || file.lastModified() > timestamp) {
        LOG.debug("Removing old log file" + file);
        boolean success = file.delete();
        if (!success) {
          throw new IOException("Unable to delete file:" + file);
        }
      }
    }
  }

}
