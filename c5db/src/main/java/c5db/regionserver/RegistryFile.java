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
