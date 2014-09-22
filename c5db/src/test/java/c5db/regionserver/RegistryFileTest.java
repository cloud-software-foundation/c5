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

package c5db.regionserver;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;

public class RegistryFileTest {

  RegistryFile registryFile;

  @Before
  public void setup() {
    try {
      registryFile = new RegistryFile(Paths.get("/tmp"));
      registryFile.truncate();
    } catch (FileNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  @Test
  public void addToFileAndReadBack() throws IOException {
//    for (int i = 0; i != 10; i++) {
//      registryFile.addEntry(getFakeHRegionInfo(i), getCF(i));
//    }
//    Map<HRegionInfo, List<HColumnDescriptor>> registry = registryFile.getRegistry();
//    assertTrue(registry.size() == 7);
//    assertTrue(registry.get(getFakeHRegionInfo(3)).size() == 4);
  }

  private HColumnDescriptor getCF(Integer i) {
    return new HColumnDescriptor(Bytes.toBytes(i.toString()));
  }

  private HRegionInfo getFakeHRegionInfo(Integer i) {
    if (i % 3 == 0) {
      TableName tableName = TableName.valueOf("fake");
      byte[] startKey = Bytes.toBytes("astart");
      byte[] endKey = Bytes.toBytes("zend");
      long regionId = 0;
      return new HRegionInfo(tableName, startKey, endKey, false, regionId);
    } else {
      TableName tableName = TableName.valueOf("fake" + i.toString());
      byte[] startKey = Bytes.toBytes("astart" + i.toString());
      byte[] endKey = Bytes.toBytes("zend" + i.toString());
      return new HRegionInfo(tableName, startKey, endKey);
    }
  }

}
