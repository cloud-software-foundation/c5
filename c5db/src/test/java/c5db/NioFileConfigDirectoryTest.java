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
