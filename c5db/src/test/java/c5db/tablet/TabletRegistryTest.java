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
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.util.C5FiberFactory;
import c5db.util.JUnitRuleFiberExceptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.jetlang.fibers.PoolFiberFactory;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;

import static c5db.util.PoolFiberFactoryWithExecutor.factoryWithExceptionHandler;

/**
 *
 */
public class TabletRegistryTest {
  private static final String ROOT_QUORUM_NAME = "root";
  @Rule
  public JUnitRuleMockery context = new JUnitRuleMockery();
  @Rule
  public JUnitRuleFiberExceptions fiberExceptionRule = new JUnitRuleFiberExceptions();
  final PoolFiberFactory poolFiberFactory = new PoolFiberFactory(Executors.newFixedThreadPool(3));
  final C5FiberFactory c5FiberFactory = factoryWithExceptionHandler(poolFiberFactory, fiberExceptionRule);

  final ConfigDirectory configDirectory = context.mock(ConfigDirectory.class);
  final TabletFactory tabletFactory = context.mock(TabletFactory.class);
  final TabletModule.Tablet rootTablet = context.mock(TabletModule.Tablet.class);

  final ReplicationModule replicationModule = context.mock(ReplicationModule.class);
  final Region.Creator regionCreator = context.mock(Region.Creator.class);

  final Configuration legacyConf = HBaseConfiguration.create();
  /**** value types ****/

  final HRegionInfo rootRegionInfo = MetaTableNames.rootRegionInfo();
  final byte[] regionInfoBytes = rootRegionInfo.toByteArray();

  final HTableDescriptor rootTableDescriptor = MetaTableNames.rootTableDescriptor();
  final byte[] rootTableDescriptorBytes = rootTableDescriptor.toByteArray();

  final List<Long> peerList = ImmutableList.of(1L, 2L, 3L);

  /*** object under test ***/
  TabletRegistry tabletRegistry;


  @Before
  public void before() throws IOException {
    context.checking(new Expectations(){{
      allowing(configDirectory).readBinaryData(with(any(String.class)), with(equal(ConfigDirectory.regionInfoFile)));
      will(returnValue(regionInfoBytes));

      allowing(configDirectory).readBinaryData(with(any(String.class)), with(equal(ConfigDirectory.htableDescriptorFile)));
      will(returnValue(rootTableDescriptorBytes));

      allowing(configDirectory).readPeers(with(any(String.class)));
      will(returnValue(peerList));

      allowing(configDirectory).configuredQuorums();
      will(returnValue(Lists.newArrayList(ROOT_QUORUM_NAME)));

      allowing(configDirectory).getBaseConfigPath();

      oneOf(tabletFactory).create(with(equal(rootRegionInfo)),
          with(equal(rootTableDescriptor)),
          with(peerList),
          with.is(anything()), /* base path */
          with.is(anything()), /* legacy conf */
          with.is(anything()), /* tablet fiber */
          with(same(replicationModule)),
          with(same(regionCreator)));
      will(returnValue(rootTablet));

      oneOf(rootTablet).start();
    }});

    tabletRegistry = new TabletRegistry(
        configDirectory,
        legacyConf,
        c5FiberFactory,
        tabletFactory,
        replicationModule,
        regionCreator);
  }

  @After
  public void after() {
  }

  @Test
  public void shouldReadFilesFromDiskThenStartTabletsDescribedTherin() throws Exception {
    tabletRegistry.startOnDiskRegions();
  }
}
