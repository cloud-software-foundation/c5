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
package c5db.regionserver;

import c5db.generated.RegionRegistryLine;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

/**
 * A Local file storing which regions are on this server.  Ideally this is self
 * discovered via some other mechanism.
 */
public class RegistryFile {
    private static final Logger LOG = LoggerFactory.getLogger(RegistryFile.class);

    private final Path c5Path;
    private File registryFile;

    public RegistryFile(Path c5Path) throws IOException {
        this.c5Path = c5Path;
        if (!(c5Path.toFile().exists() && c5Path.toFile().isDirectory())) {
            boolean success = c5Path.toFile().mkdirs();
            if (!success) {
                throw new IOException("Unable to create c5Path:" + c5Path);
            }

        }

        this.registryFile = new File(c5Path.toString(), "REGISTRY");
        if (!registryFile.exists()) {
            boolean success = registryFile.createNewFile();
            if (!success) {
                throw new IOException("Unable to create registry file");
            }
        }
    }

    public void truncate() throws IOException {
        boolean success = this.registryFile.delete();
        if (!success) {
            throw new IOException("Unable to delete registry file");
        }
        this.registryFile = new File(c5Path.toString(), "REGISTRY");
    }

    public void addEntry(HRegionInfo hRegionInfo, HColumnDescriptor cf,
                         List<Long> peers)
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
                        .addCf(ByteString.copyFrom(cf.getName()))
                        .addAllPeers(peers)
                        .build();
        regionRegistryLine.writeDelimitedTo(fileOutputStream);
        fileOutputStream.flush();
        fileOutputStream.getFD().sync();
    }

    public static class Registry {
        public final ImmutableMap<HRegionInfo, ImmutableList<HColumnDescriptor>> regions;
        public final ImmutableMap<HRegionInfo, ImmutableList<Long>> peers;

        public Registry(ImmutableMap<HRegionInfo, ImmutableList<HColumnDescriptor>> regions,
                        ImmutableMap<HRegionInfo, ImmutableList<Long>> peers) {
            this.regions = regions;
            this.peers = peers;
        }
    }

    public Registry getRegistry()
            throws IOException {
        HashMap<HRegionInfo, ImmutableList<HColumnDescriptor>> registry = new HashMap<>();
        HashMap<HRegionInfo, ImmutableList<Long>> peers = new HashMap<>();

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
                        TableName.valueOf(entry.getTableName().toByteArray()),
                        entry.getStartKey().toByteArray(),
                        entry.getEndKey().toByteArray(),
                        false,
                        entry.getRegionId());


                if (registry.containsKey(regionInfo)) {
                    LOG.warn("Duplicate region info in registry file: {}, second one ignored",
                            regionInfo);
                    continue;
                }

                ImmutableList.Builder<HColumnDescriptor> lstBuild = ImmutableList.builder();
                for (ByteString cf: entry.getCfList()) {
                    lstBuild.add(new HColumnDescriptor(cf.toByteArray()));
                }

                registry.put(regionInfo, lstBuild.build());
                peers.put(regionInfo, ImmutableList.copyOf(entry.getPeersList()));
            }
            while (entry != null);
        }
        return new Registry(ImmutableMap.copyOf(registry), ImmutableMap.copyOf(peers));
    }
}
