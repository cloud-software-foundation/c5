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

package c5db.tablet;

import c5db.ConfigDirectory;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetlang.channels.Channel;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class TabletRegistryTest {
  private static final Channel<TabletStateChange> DO_NOT_CARE_STATE_CHANGE_CHANNEL = null;
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();

  private final C5Server c5server = context.mock(C5Server.class);
  private final ConfigDirectory configDirectory = context.mock(ConfigDirectory.class);
  private final TabletFactory tabletFactory = context.mock(TabletFactory.class);
  private final c5db.interfaces.tablet.Tablet rootTablet = context.mock(Tablet.class);

  private final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  private final Region.Creator regionCreator = context.mock(Region.Creator.class);

  private final Configuration legacyConf = HBaseConfiguration.create();

  /**
   * * value types ***
   */
  private final HRegionInfo rootRegionInfo = SystemTableNames.rootRegionInfo();
  private final byte[] regionInfoBytes = rootRegionInfo.toByteArray();

  private final HTableDescriptor rootTableDescriptor = SystemTableNames.rootTableDescriptor();
  private final byte[] rootTableDescriptorBytes = rootTableDescriptor.toByteArray();

  private final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);

  private final String ROOT_QUORUM_NAME = rootRegionInfo.getRegionNameAsString();

  /**
   * object under test **
   */
  private TabletRegistry tabletRegistry;

  @Before
  public void before() throws IOException {
    context.checking(new Expectations() {{
      oneOf(tabletFactory).create(
          with(equal(c5server)),
          with(equal(rootRegionInfo)),
          with(equal(rootTableDescriptor)),
          with(peerList),
          with.is(anything()), /* base path */
          with.is(anything()), /* legacy conf */
          with(same(replicationModule)),
          with(same(regionCreator)));
      will(returnValue(rootTablet));

      oneOf(rootTablet).setStateChangeChannel(DO_NOT_CARE_STATE_CHANGE_CHANNEL);
      oneOf(rootTablet).start();
    }});

    tabletRegistry = new TabletRegistry(
        c5server,
        configDirectory,
        legacyConf,
        DO_NOT_CARE_STATE_CHANGE_CHANNEL,
        replicationModule, tabletFactory,
        regionCreator);
  }

  @Test
  public void shouldReadFilesFromDiskThenStartTabletsDescribedThereIn() throws Exception {
    context.checking(new Expectations() {{
      // Base configuration directory information
      allowing(configDirectory).readBinaryData(with(any(String.class)), with(equal(ConfigDirectory.regionInfoFile)));
      will(returnValue(regionInfoBytes));

      allowing(configDirectory).readBinaryData(with(any(String.class)), with(equal(ConfigDirectory.htableDescriptorFile)));
      will(returnValue(rootTableDescriptorBytes));

      allowing(configDirectory).readPeers(with(any(String.class)));
      will(returnValue(peerList));

      allowing(configDirectory).configuredQuorums();
      will(returnValue(Lists.newArrayList(ROOT_QUORUM_NAME)));

      allowing(configDirectory).getBaseConfigPath();
    }});
    tabletRegistry.startOnDiskRegions();
  }

  @Test
  public void shouldStartTabletWhenRequestedTo() throws Throwable {
    context.checking(new Expectations() {{
      oneOf(configDirectory).writePeersToFile(ROOT_QUORUM_NAME, peerList);
      oneOf(configDirectory).writeBinaryData(ROOT_QUORUM_NAME, ConfigDirectory.htableDescriptorFile,
          rootTableDescriptorBytes);
      oneOf(configDirectory).writeBinaryData(ROOT_QUORUM_NAME, ConfigDirectory.regionInfoFile, regionInfoBytes);
      allowing(configDirectory).getBaseConfigPath();
    }});

    tabletRegistry.startTablet(rootRegionInfo, rootTableDescriptor, peerList);
  }
}
