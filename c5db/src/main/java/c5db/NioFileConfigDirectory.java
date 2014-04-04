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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class NioFileConfigDirectory implements ConfigDirectory {
  private static final Logger LOG = LoggerFactory.getLogger(NioFileConfigDirectory.class);

  private static final Charset UTF_8 = Charset.forName("UTF-8");

  private final Path baseConfigPath;
  private final Path nodeIdPath;
  private final Path clusterNamePath;

  public NioFileConfigDirectory(Path baseConfigPath) throws Exception {
    this.baseConfigPath = baseConfigPath;
    this.nodeIdPath = baseConfigPath.resolve(nodeIdFile);
    this.clusterNamePath = baseConfigPath.resolve(clusterNameFile);

    init();
  }

  /**
   * Verifies that the 'config directory' is actually usable.  If it doesn't exist, create it.  If it exists,
   * ensure that it's writable.  Ensure that primary configuration files aren't directories.
   *
   * @throws Exception
   */
  private void init() throws Exception {
    if (Files.exists(getBaseConfigPath()) && !Files.isDirectory(getBaseConfigPath())) {
      throw new Exception("Base config path exists and is not a directory " + getBaseConfigPath());
    }

    if (!Files.exists(getBaseConfigPath())) {
      Files.createDirectories(getBaseConfigPath());
    }


    if (Files.exists(nodeIdPath) && !Files.isRegularFile(nodeIdPath)) {
      throw new Exception("NodeId file is not a regular directory!");
    }

    if (Files.exists(clusterNamePath) && !Files.isRegularFile(clusterNamePath)) {
      throw new Exception("Cluster name is not a regular directory!");
    }

    if (!Files.isWritable(getBaseConfigPath())) {
      throw new Exception("Can't write to the base configuration path!");
    }
  }

  /** Get the contents of the node id config file */
  @Override
  public String getNodeId() throws IOException {
    return getFirstLineOfFile(nodeIdPath);
  }

  @Override
  public String getClusterName() throws IOException {
    return getFirstLineOfFile(clusterNamePath);
  }

  @Override
  public void createSubDir(Path dirRelPath) throws IOException {
    if (dirRelPath.isAbsolute()) {
      throw new IllegalArgumentException("dirRelPath should a relative path with respect to the base config directory");
    }
    Path dirPath = this.getBaseConfigPath().resolve(dirRelPath);
    if (Files.isRegularFile(dirPath)) {
      throw new IOException("dirPath is a regular file! It needs to be a directory: " + dirPath);
    }
    // not a regular file.
    if (Files.isDirectory(dirPath) && !Files.isWritable(dirPath)) {
      throw new IOException("dirPath is a directory but not writable by me: " + dirPath);
    }

    if (Files.isDirectory(dirPath)) {
      return;
    }
    Files.createDirectories(dirPath);
  }

  @Override
  public void writeFile(Path dirRelPath, String fileName, List<String> data) throws IOException {
    createSubDir(dirRelPath);
    Path filePath = getBaseConfigPath().resolve(dirRelPath).resolve(fileName);
    Files.write(filePath, data, UTF_8);
  }

  @Override
  public List<String> readFile(Path dirRelPath, String fileName) throws IOException {
    Path filePath = getBaseConfigPath().resolve(dirRelPath).resolve(fileName);
    try {
      return Files.readAllLines(filePath, UTF_8);
    } catch (NoSuchFileException ex) {
      // file doesn't exist, return empty:
      return new ArrayList<>();
    }
  }

  private String getFirstLineOfFile(Path path) throws IOException {
    if (Files.isRegularFile(path)) {
      List<String> allLines;

      try {
        allLines = Files.readAllLines(path, UTF_8);
      } catch (NoSuchFileException ex) {
        return null;
      }
      if (allLines.isEmpty())
        return null;
      return allLines.get(0);
    }
    return null;
  }

  @Override
  public void setNodeIdFile(String data) throws IOException {
    setFile(data, nodeIdPath);
  }
  @Override
  public void setClusterNameFile(String data) throws IOException {
    setFile(data, clusterNamePath);
  }

  private void setFile(String data, Path path) throws IOException {
    List<String> lines = new ArrayList<>(1);
    lines.add(data);
    Files.write(path, lines, UTF_8);
  }

  @Override
  public Path getQuorumRelPath(String quorumId) {
    return Paths.get(quorumsSubDir, quorumId);
  }

  @Override
  public void writePeersToFile(String quorumId, List<Long> peers) throws IOException {
    //noinspection Convert2MethodRef
    List<String> peerIdsStrings = Lists.transform(peers, (p) -> p.toString());
    writeFile(getQuorumRelPath(quorumId), peerIdsFile, peerIdsStrings);
  }

  @Override
  public void writeBinaryData(String quorumId, byte[] data) throws IOException {
    Path quorumRelPath = getQuorumRelPath(quorumId);
    createSubDir(quorumRelPath);
    Path filePath = getBaseConfigPath().resolve(quorumRelPath).resolve(regionInfoFile);
    Files.write(filePath, data);
  }

  @Override
  public Path getBaseConfigPath() {
    return baseConfigPath;
  }
}
