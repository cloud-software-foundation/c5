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

package c5db.tablet;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helper functionality to make various value type/objects.
 */
public class SystemTableNames {

  public static byte[] sep = Bytes.toBytes(",");

  public static HTableDescriptor rootTableDescriptor() {
    return HTableDescriptor.ROOT_TABLEDESC;
  }

  public static HTableDescriptor metaTableDescriptor() {
    return HTableDescriptor.META_TABLEDESC;
  }

  public static TableName metaTableName() {
    return TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "meta");
  }

  public static TableName rootTableName() {
    return HTableDescriptor.ROOT_TABLE_NAME;
  }

  public static HRegionInfo rootRegionInfo() {
    return new HRegionInfo(
        rootTableName(),
        startKeyZero(),
        endKeyEmpty(),
        false,
        rootRegionId()
    );
  }

  private static int rootRegionId() {
    return 1;
  }

  private static int metaRegionId() {
    return 2;
  }

  private static byte[] startKeyZero() {
    return new byte[]{};
  }

  private static byte[] endKeyEmpty() {
    return new byte[]{};
  }

  public static HRegionInfo metaRegionInfo() {
    return new HRegionInfo(
        metaTableName(),
        startKeyZero(),
        endKeyEmpty(),
        false,
        metaRegionId()
    );
  }
}