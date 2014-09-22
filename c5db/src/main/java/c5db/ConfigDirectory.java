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

package c5db;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 *
 */
public interface ConfigDirectory {
  String nodeIdFile = "nodeId";
  String clusterNameFile = "clusterName";
  String quorumsSubDir = "repl";
  String peerIdsFile = "peerIds";
  String regionInfoFile = "region-info";
  String htableDescriptorFile = "htable-descriptor";

  /**
   * Get the contents of the node id config file
   */
  String getNodeId() throws IOException;

  String getClusterName() throws IOException;

  void createSubDir(Path dirRelPath) throws IOException;

  void writeFile(Path dirRelPath, String fileName, List<String> data) throws IOException;

  List<String> readFile(Path dirRelPath, String fileName) throws IOException;

  void setNodeIdFile(String data) throws IOException;

  void setClusterNameFile(String data) throws IOException;

  // TODO refactor and remove this, replace it with more specific methods.
  Path getQuorumRelPath(String quorumId) throws IOException;


  /* Quorum config persistence methods */

  List<Long> readPeers(String quorumId) throws IOException;

  void writePeersToFile(String quorumId, List<Long> peers) throws IOException;

  void writeBinaryData(String quorumId, String type, byte[] data) throws IOException;

  byte[] readBinaryData(String quorumId, String type) throws IOException;

  /**
   * Returns an enumeration of the readable quorums on disk.  Each quorum is configured as a
   * directory, and this just returns the list of all directories in the quorum config dir.
   *
   * @return the list of quorums configured for this ConfigDirectory
   * @throws IOException underlying IO errors
   */
  List<String> configuredQuorums() throws IOException;

  // TODO reduce the number of callers who can call this
  Path getBaseConfigPath();
}
