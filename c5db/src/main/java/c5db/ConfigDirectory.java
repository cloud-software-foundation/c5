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
import java.nio.file.Path;
import java.util.List;

/**
 * Created by ryan on 3/25/14.
 */
public interface ConfigDirectory {
  String nodeIdFile = "nodeId";
  String clusterNameFile = "clusterName";
  String quorumsSubDir = "repl";
  String peerIdsFile = "peerIds";
  String regionInfoFile = "region-info";
  String persisterFile = "replication-data";

  /** Get the contents of the node id config file */
  String getNodeId() throws IOException;

  String getClusterName() throws IOException;

  void createSubDir(Path dirRelPath) throws IOException;

  void writeFile(Path dirRelPath, String fileName, List<String> data) throws IOException;

  List<String> readFile(Path dirRelPath, String fileName) throws IOException;

  void setNodeIdFile(String data) throws IOException;

  void setClusterNameFile(String data) throws IOException;

  Path getQuorumRelPath(String quorumId);

  void writePeersToFile(String quorumId, List<Long> peers) throws IOException;

  void writeBinaryData(String quorumId, byte[] data) throws IOException;
}
