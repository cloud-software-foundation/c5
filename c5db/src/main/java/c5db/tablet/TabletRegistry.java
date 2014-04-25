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
import c5db.interfaces.TabletModule;
import c5db.util.C5FiberFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.jetlang.channels.Channel;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Handles the logic of starting quorums, restoring them from disk, etc.
 * <p/>
 * Totally NOT thread safe!
 */
public class TabletRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TabletRegistry.class);

  private final C5FiberFactory fiberFactory;
  private final TabletFactory tabletFactory;

  private final Region.Creator regionCreator;

  private final Map<String, TabletModule.Tablet> tablets = new HashMap<>();
  private final ReplicationModule replicationModule;
  private final C5Server c5server;
  private final ConfigDirectory configDirectory;
  private final Configuration legacyConf;

  public TabletRegistry(C5Server c5server,
                        ConfigDirectory configDirectory,
                        Configuration legacyConf,
                        C5FiberFactory fiberFactory,
                        ReplicationModule replicationModule,
                        TabletFactory tabletFactory,
                        Region.Creator regionCreator) {
    this.c5server = c5server;
    this.configDirectory = configDirectory;
    this.legacyConf = legacyConf;
    this.fiberFactory = fiberFactory;
    this.replicationModule = replicationModule;
    this.tabletFactory = tabletFactory;
    this.regionCreator = regionCreator;
  }

  public void startOnDiskRegions() throws IOException {
    List<String> quorums = configDirectory.configuredQuorums();

    // for each quorum, _start_ it or something.
    for (String quorum : quorums) {

      try {
        List<Long> peers = configDirectory.readPeers(quorum);
        byte[] regionInfoBytes = configDirectory.readBinaryData(quorum, ConfigDirectory.regionInfoFile);
        byte[] tableDescriptorBytes = configDirectory.readBinaryData(quorum, ConfigDirectory.htableDescriptorFile);
        HRegionInfo regionInfo = HRegionInfo.parseFrom(regionInfoBytes);
        HTableDescriptor tableDescriptor = HTableDescriptor.parseFrom(tableDescriptorBytes);

        Path basePath = configDirectory.getBaseConfigPath();

        TabletModule.Tablet tablet = tabletFactory.create(
            c5server,
            regionInfo,
            tableDescriptor,
            peers,
            basePath,
            legacyConf,
            fiberFactory.create(),
            replicationModule,
            regionCreator);

        tablet.start();

        tablets.put(quorum, tablet);
      } catch (IOException | DeserializationException e) {
        LOG.error("Unable to start quorum, due to config error: " + quorum, e);
      }
    }
  }

  public TabletModule.Tablet startTablet(HRegionInfo regionInfo,
                                         HTableDescriptor tableDescriptor,
                                         List<Long> peerList) throws IOException, InterruptedException {
    Path basePath = configDirectory.getBaseConfigPath();

    // quorum name - ?
    String quorumName = regionInfo.getRegionNameAsString();
    if (tablets.containsKey(quorumName)) {
      // cant start, already started:
      LOG.warn("Trying to start tablet {} already started!", quorumName);
      return tablets.get(quorumName);
    }

    // write the stuff to disk first:
    configDirectory.writeBinaryData(quorumName, ConfigDirectory.regionInfoFile,
        regionInfo.toByteArray());
    configDirectory.writeBinaryData(quorumName, ConfigDirectory.htableDescriptorFile,
        tableDescriptor.toByteArray());
    configDirectory.writePeersToFile(quorumName, peerList);

    Fiber tabletFiber = fiberFactory.create();
    TabletModule.Tablet newTablet = tabletFactory.create(
        c5server,
        regionInfo,
        tableDescriptor,
        peerList,
        basePath,
        legacyConf,
        tabletFiber,
        replicationModule,
        regionCreator);
    tablets.put(quorumName, newTablet);

    final CountDownLatch latch = new CountDownLatch(1);
    Channel<TabletModule.TabletStateChange> channel = newTablet.getStateChangeChannel();
    channel.subscribe(tabletFiber, message -> {
      if (message.state.equals(TabletModule.Tablet.State.Open)) {
        latch.countDown();
      }
    });

    newTablet.start();
    latch.await();
    return newTablet;
  }

  Map<String, TabletModule.Tablet> getTablets() {
    return tablets;
  }
}
