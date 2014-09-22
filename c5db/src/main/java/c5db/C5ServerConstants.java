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

import org.apache.hadoop.hbase.util.Bytes;

public class C5ServerConstants {
  public static final int MAX_CALL_SIZE = Integer.MAX_VALUE;
  public static final long MAX_CONTENT_LENGTH_HTTP_AGG = 8192;

  public static final String LOCALHOST = "localhost";
  public static final java.lang.String MIN_CLUSTER_SIZE = "minClusterSize";
  public static final int MINIMUM_DEFAULT_QUORUM_SIZE = 3;
  public static final int DEFAULT_QUORUM_SIZE = 3;
  public static final String C5_CFG_PATH = "c5.cfgPath";

  // We use this column qualifier in system tables to mark the leader
  public static final byte[] LEADER_QUALIFIER = Bytes.toBytes("LEADER_QUALIFIER");

  // Commands we pass to the command server
  public static final String START_META = "Start Meta";
  public static final String CREATE_TABLE = "Create Table";
  public static final String LAUNCH_TABLET = "Launch Tablet";
  public static final String SET_META_LEADER = "Set me as Meta Leader";
  public static final String SET_USER_LEADER = "Set me as User Leader";

  public static final String LOOPBACK_ADDRESS = "127.0.0.1";
  public static final String BROADCAST_ADDRESS = "255.255.255.255";
  public static final int DISCOVERY_PORT = 54333;
  public static final int DEFAULT_WEB_SERVER_PORT = 31337;
  public static final int CONTROL_RPC_PROPERTY_PORT = 9099;

  public static final int REPLICATOR_PORT_MIN = 1024;
  public static final int REPLICATOR_PORT_RANGE = 30000;

  public static final int DEFAULT_REGION_SERVER_PORT_MIN = 8080;
  public static final int REGION_SERVER_PORT_RANGE = 1000;

  public static final String CLUSTER_NAME_PROPERTY_NAME = "clusterName";
  public static final String WEB_SERVER_PORT_PROPERTY_NAME = "webServerPort";
  public static final String REGION_SERVER_PORT_PROPERTY_NAME = "regionServerPort";
  public static final String CONTROL_SERVER_PORT_PROPERTY_NAME = "controlServerPort";
}
