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

import c5db.interfaces.C5Server;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import sun.misc.BASE64Encoder;

import java.util.List;

public class TestHelpers {
  private static HRegionInfo testRegion;
  private static HTableDescriptor testDesc;
  private static long peer;

  public static String getCreateTabletCommand(final TableName tableName, final long nodeId) {
    peer = nodeId;
    testDesc = new HTableDescriptor(tableName);
    testDesc.addFamily(new HColumnDescriptor("cf"));
    testRegion = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);
    String peerString = String.valueOf(nodeId);
    BASE64Encoder encoder = new BASE64Encoder();

    String hTableDesc = encoder.encodeBuffer(testDesc.toByteArray());
    String hRegionInfo = encoder.encodeBuffer(testRegion.toByteArray());

    return C5ServerConstants.LAUNCH_TABLET + ":" + hTableDesc + "," + hRegionInfo + "," + peerString;

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
