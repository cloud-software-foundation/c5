/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.regionserver;

import com.google.protobuf.ByteString;
import ohmdb.generated.RegionRegistryLine;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RegistryFile {

  private final String ohmPath;
  private File registryFile;

  public RegistryFile(String ohmPath) throws IOException {
    this.ohmPath = ohmPath;
    this.registryFile = new File(ohmPath, "REGISTRY");
    if (!registryFile.exists()) {
      boolean success = registryFile.createNewFile();
      if (!success){
        throw new IOException("Unable to create registry file");
      }
    }
  }

  public void truncate() throws IOException {
    boolean success = this.registryFile.delete();
    if (!success){
      throw new IOException("Unable to delete registry file");
    }
    this.registryFile = new File(ohmPath, "REGISTRY");
  }

  public void addEntry(HRegionInfo hRegionInfo, HColumnDescriptor cf)
      throws IOException {
    FileOutputStream fileOutputStream = new FileOutputStream(registryFile,
        true);

    RegionRegistryLine.RegistryLine regionRegistryLine =
        RegionRegistryLine
            .RegistryLine
            .newBuilder()
            .setTableName(ByteString.copyFrom(hRegionInfo.getTableName()))
            .setStartKey(ByteString.copyFrom(hRegionInfo.getStartKey()))
            .setEndKey(ByteString.copyFrom(hRegionInfo.getEndKey()))
            .setRegionId(hRegionInfo.getRegionId())
            .setCf(ByteString.copyFrom(cf.getName()))
            .build();
    regionRegistryLine.writeDelimitedTo(fileOutputStream);
    fileOutputStream.flush();
    fileOutputStream.getFD().sync();
  }

  public Map<HRegionInfo, List<HColumnDescriptor>> getRegistry()
      throws IOException {
    HashMap<HRegionInfo, List<HColumnDescriptor>> registry = new HashMap<>();

    if (registryFile.length() != 0) {
      FileInputStream fileInputStream = new FileInputStream(registryFile);
      RegionRegistryLine.RegistryLine entry;
      do {
        entry = RegionRegistryLine
            .RegistryLine
            .parseDelimitedFrom(fileInputStream);
        if (entry == null) {
          continue;
        }

        HRegionInfo regionInfo = new HRegionInfo(
            entry.getTableName().toByteArray(),
            entry.getStartKey().toByteArray(),
            entry.getEndKey().toByteArray(),
            false,
            entry.getRegionId());
        HColumnDescriptor hc = new HColumnDescriptor(entry.getCf().toByteArray());

        if (!registry.containsKey(regionInfo)) {
          registry.put(regionInfo, new ArrayList<HColumnDescriptor>());
        }
        registry.get(regionInfo).add(hc);
      }
      while (entry != null);
    }
    return registry;
  }
}
