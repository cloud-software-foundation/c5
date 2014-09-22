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
package c5db.util;

import c5db.client.generated.TableName;
import io.protostuff.ByteString;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

public class TabletNameHelpers {
  public static TableName getClientTableName(org.apache.hadoop.hbase.TableName hbaseTableName) {
    ByteBuffer namespace = ByteBuffer.wrap(hbaseTableName.getNamespace());
    ByteBuffer qualifier = ByteBuffer.wrap(hbaseTableName.getQualifier());
    return new TableName(namespace, qualifier);
  }

  public static org.apache.hadoop.hbase.TableName getHBaseTableName(TableName hbaseTableName) {
    return org.apache.hadoop.hbase.TableName.valueOf(hbaseTableName.getNamespace(), hbaseTableName.getQualifier());
  }

  public static TableName getClientTableName(String namespaceIn, String qualifierIn) {
    ByteBuffer namespace = ByteBuffer.wrap(Bytes.toBytes(namespaceIn));
    ByteBuffer qualifier = ByteBuffer.wrap(Bytes.toBytes(qualifierIn));
    return new TableName(namespace, qualifier);
  }

  public static String toString(TableName tableName) {
    return Bytes.toString(tableName.getNamespace().array()) + ":" + Bytes.toString(tableName.getQualifier().array());
  }

  public static byte[] toBytes(TableName tableName) {
    return Bytes.add(tableName.getNamespace().array(), Bytes.toBytes(":"), tableName.getQualifier().array());
  }

  public static ByteString toByteString(TableName tableName) {
    return ByteString.copyFrom(toBytes(tableName));
  }
}