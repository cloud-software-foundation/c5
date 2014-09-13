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