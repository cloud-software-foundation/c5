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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

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
}
