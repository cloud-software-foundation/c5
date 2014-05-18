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

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class NioFileConfigDirectoryTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  ConfigDirectory cfgDir;
  File tmpFolder;
  private HTableDescriptor metaDesc;
  private HRegionInfo metaRegion;
  private List<Long> peers;

  @Before
  public void setUp() throws Exception {
    folder.create();
    tmpFolder = folder.newFolder();
    cfgDir = new NioFileConfigDirectory(tmpFolder.toPath());
    cfgDir.setNodeIdFile("1l");

    metaDesc = HTableDescriptor.META_TABLEDESC;
    metaRegion = new HRegionInfo(metaDesc.getTableName(), new byte[]{0}, new byte[]{}, false, 1);
    peers = Arrays.asList(1l, 2l, 3l);
    String quorumName = metaRegion.getRegionNameAsString();

    // write the stuff to disk first:
    cfgDir.writeBinaryData(quorumName, ConfigDirectory.regionInfoFile,
        metaRegion.toByteArray());
    cfgDir.writeBinaryData(quorumName, ConfigDirectory.htableDescriptorFile,
        metaDesc.toByteArray());
    cfgDir.writePeersToFile(quorumName, peers);

  }

  @Test
  public void shouldBeAbleToReadEntriesBack() throws Exception {
    cfgDir = new NioFileConfigDirectory(tmpFolder.toPath());
    assertThat(cfgDir.getNodeId(), is(equalTo("1l")));
    List<String> quorums = cfgDir.configuredQuorums();
    assertThat(quorums.size(), is(equalTo(1)));

    String quorum = quorums.iterator().next();
    assertThat(cfgDir.readPeers(quorum), is(equalTo(peers)));
    assertThat(cfgDir.readBinaryData(quorum, ConfigDirectory.regionInfoFile), is(equalTo(metaRegion.toByteArray())));
    assertThat(cfgDir.readBinaryData(quorum, ConfigDirectory.htableDescriptorFile), is(equalTo(metaDesc.toByteArray())));

  }
}
