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
