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

import c5db.interfaces.C5Server;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import sun.misc.BASE64Encoder;

import java.util.List;
import java.util.function.Consumer;

public class TestHelpers {
  public static HRegionInfo testRegion;
  public static HTableDescriptor testDesc;
  public static long peer;

  public static String getCreateTabletCommand(final TableName tableName, final long nodeId) {
    peer = nodeId;
    testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("cf"));
    testRegion = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);
    String peerString = String.valueOf(nodeId);
    BASE64Encoder encoder = new BASE64Encoder();

    String hTableDesc = encoder.encodeBuffer(testDesc.toByteArray());
    String hRegionInfo = encoder.encodeBuffer(testRegion.toByteArray());

    return C5ServerConstants.CREATE_TABLE + ":" + hTableDesc + "," + hRegionInfo + "," + peerString;

  }

  public static String setMetaLeader(final long l) {
    return C5ServerConstants.SET_META_LEADER + ":" + l;

  }

  public static String getCreateTabletSubCommand(String namespace,
                                                 String qualifier,
                                                 byte[][] splitkeys,
                                                 List<C5Server> servers) {
    return getCreateTabletSubCommand(TableName.valueOf(namespace, qualifier), splitkeys, servers);
  }

  public static String getCreateTabletSubCommand(TableName tableName, byte[][] splitkeys, List<C5Server> servers) {
    HTableDescriptor testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("cf"));

    StringBuilder hRegionInfos = new StringBuilder();
    BASE64Encoder encoder = new BASE64Encoder();

    byte[] previousEndKey = new byte[0];
    long counter = 1;

    for (byte[] splitKey : splitkeys) {
      HRegionInfo hRegionInfo = new HRegionInfo(tableName, previousEndKey, splitKey, false, counter++);
      previousEndKey = splitKey;
      hRegionInfos.append(encoder.encodeBuffer(hRegionInfo.toByteArray()));
      hRegionInfos.append(",");
    }

    HRegionInfo hRegionInfo = new HRegionInfo(tableName, previousEndKey, new byte[0], false, counter);
    hRegionInfos.append(encoder.encodeBuffer(hRegionInfo.toByteArray()));
    hRegionInfos.append(",");

    String peerString = String.valueOf(servers.get(0).getNodeId());
    for (C5Server server : servers.subList(1, servers.size())) {
      peerString += "," + server.getNodeId();
    }

    String hTableDesc = encoder.encodeBuffer(testDesc.toByteArray());

    String hRegionInfoSplits = hRegionInfos.toString();
    // remove the last comma
    hRegionInfoSplits = hRegionInfoSplits.substring(0, hRegionInfos.length() - 1);
    return C5ServerConstants.CREATE_TABLE + ":" + hTableDesc + "," + hRegionInfoSplits + "," + peerString;
  }
}
