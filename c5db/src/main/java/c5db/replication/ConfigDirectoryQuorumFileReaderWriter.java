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

package c5db.replication;

import c5db.ConfigDirectory;

import java.io.IOException;
import java.util.List;

public class ConfigDirectoryQuorumFileReaderWriter implements QuorumFileReaderWriter {
  private final ConfigDirectory configDirectory;

  public ConfigDirectoryQuorumFileReaderWriter(ConfigDirectory configDirectory) {
    this.configDirectory = configDirectory;
  }

  @Override
  public List<String> readQuorumFile(String quorumId, String fileName) throws IOException {
    return configDirectory.readFile(configDirectory.getQuorumRelPath(quorumId), fileName);
  }

  @Override
  public void writeQuorumFile(String quorumId, String fileName, List<String> data) throws IOException {
    configDirectory.writeFile(configDirectory.getQuorumRelPath(quorumId), fileName, data);
  }
}
