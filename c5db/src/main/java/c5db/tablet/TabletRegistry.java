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

import c5db.C5Compare;
import c5db.ConfigDirectory;
import c5db.client.generated.TableName;
import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.regionserver.RegionNotFoundException;
import c5db.util.TabletNameHelpers;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.jetlang.channels.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Handles the logic of starting quorums, restoring them from disk, etc.
 * <p>
 * Totally NOT thread safe!
 */
public class TabletRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TabletRegistry.class);
  private final TabletFactory tabletFactory;
  private final Region.Creator regionCreator;

  // contains tables, which contain tablets

  private final NonBlockingHashMap<String, ConcurrentSkipListMap<byte[], Tablet>> tables = new NonBlockingHashMap<>();
  private final Channel<TabletStateChange> commonStateChangeChannel;
  private final ReplicationModule replicationModule;
  private final C5Server c5server;
  private final ConfigDirectory configDirectory;
  private final Configuration legacyConf;

  public TabletRegistry(C5Server c5server,
                        ConfigDirectory configDirectory,
                        Configuration legacyConf,
                        Channel<TabletStateChange> commonStateChangeChannel,
                        ReplicationModule replicationModule,
                        TabletFactory tabletFactory,
                        Region.Creator regionCreator) {
    this.c5server = c5server;
    this.configDirectory = configDirectory;
    this.legacyConf = legacyConf;
    this.commonStateChangeChannel = commonStateChangeChannel;
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
        Tablet tablet = tabletFactory.create(
            c5server,
            regionInfo,
            tableDescriptor,
            peers,
            basePath,
            legacyConf,
            replicationModule,
            regionCreator);
        tablet.start();
        tablet.setStateChangeChannel(commonStateChangeChannel);


        ConcurrentSkipListMap<byte[], Tablet> tablets;
        // This is a new table to this server
        String tableName = regionInfo.getTable().getNameAsString();
        if (tables.containsKey(tableName)) {
          tablets = tables.get(tableName);
        } else {
          tablets = new ConcurrentSkipListMap<>(new C5Compare());
          TableName hbaseTableName = TabletNameHelpers.getClientTableName(regionInfo.getTable());
          tables.put(TabletNameHelpers.toString(hbaseTableName), tablets);
        }

        tablets.put(Bytes.add(Bytes.toBytes(regionInfo.getTable().toString()),
            Bytes.toBytes(","),
            regionInfo.getEndKey()), tablet);
      } catch (IOException | DeserializationException e) {
        LOG.error("Unable to start quorum, due to config error: " + quorum, e);
      }
    }
  }


  public Tablet startTablet(HRegionInfo regionInfo,
                            HTableDescriptor tableDescriptor,
                            List<Long> peerList) throws IOException {
    Path basePath = configDirectory.getBaseConfigPath();

    // quorum name - ?
    String quorumName = regionInfo.getRegionNameAsString();

    // write the stuff to disk first:
    configDirectory.writeBinaryData(quorumName, ConfigDirectory.regionInfoFile,
        regionInfo.toByteArray());
    configDirectory.writeBinaryData(quorumName, ConfigDirectory.htableDescriptorFile,
        tableDescriptor.toByteArray());
    configDirectory.writePeersToFile(quorumName, peerList);

    Tablet tablet = tabletFactory.create(
        c5server,
        regionInfo,
        tableDescriptor,
        peerList,
        basePath,
        legacyConf,
        replicationModule,
        regionCreator);
    tablet.setStateChangeChannel(commonStateChangeChannel);
    tablet.start();
    ConcurrentSkipListMap<byte[], Tablet> tablets;
    // This is a new table to this server
    String tableName = TabletNameHelpers.toString(TabletNameHelpers.getClientTableName(regionInfo.getTable()));
    byte[] rowKey;
    if (regionInfo.getEndKey().length == 0) {
      rowKey = new byte[0];
    } else {
      rowKey = regionInfo.getEndKey();
    }

    if (tables.containsKey(tableName)) {
      tablets = tables.get(tableName);
    } else {
      tablets = new ConcurrentSkipListMap<>(new C5Compare());
      tables.put(tableName, tablets);
    }
    Tablet replacement = tablets.put(rowKey, tablet);
    if (replacement != null) {
      LOG.error("We replaced a tablet inadvertently" + replacement.toString());
    }

    return tablet;
  }

  public Tablet getTablet(String tableName, byte[] row) throws RegionNotFoundException {
    return this.getTablet(tableName, ByteBuffer.wrap(row));

  }

  public Tablet getTablet(String tableName, ByteBuffer row) throws RegionNotFoundException {
    Tablet tablet;

    if (!tables.containsKey(tableName)) {
      throw new RegionNotFoundException("We couldn't find table: " + tableName);
    }

    ConcurrentSkipListMap<byte[], Tablet> tablets = tables.get(tableName);
    if (tablets.size() == 1) {
      tablet = tablets.values().iterator().next();
      //Properly formed single tablet table end tablet
      if (tablet.getRegionInfo().getEndKey().length != 0) {
        throw new RegionNotFoundException("We only have one tablet, but it has an endRow");
      }
    } else {
      byte[] sep = SystemTableNames.sep;
      // It must be the last tablet
      if (row == null || row.array().length == 0) {
        tablet = tablets.ceilingEntry(new byte[]{0x00}).getValue();
      } else {
        Map.Entry<byte[], Tablet> entry = tablets.higherEntry(row.array());
        if (entry == null) {
          tablet = tablets.ceilingEntry(sep).getValue();
        } else {
          tablet = entry.getValue();
        }
      }
    }
    assureCorrectRequest(tablet, row);
    return tablet;
  }

  private void assureCorrectRequest(Tablet tablet, ByteBuffer row) throws RegionNotFoundException {
    if (!HRegion.rowIsInRange(tablet.getRegionInfo(), row.array())) {
      throw new RegionNotFoundException("We are trying to return a region which is not in range");
    }
  }

  public Collection<Tablet> dumpTablets() {
    ArrayList<Tablet> tablets = new ArrayList<>();
    tables.values().forEach(tabletConcurrentSkipListMap
        -> tabletConcurrentSkipListMap.values().stream().forEach(tablets::add));
    return tablets;
  }

  NonBlockingHashMap<String, ConcurrentSkipListMap<byte[], Tablet>> getTables() {
    return tables;
  }

  public ConcurrentSkipListMap<byte[], Tablet> getTablets(String tableName) {
        return this.tables.get(tableName);
  }
}