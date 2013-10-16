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
package ohmdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class ConfigDirectory {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigDirectory.class);

    public final static String nodeIdFile = "nodeId";
    public final static String clusterNameFile = "clusterName";
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    public final Path baseConfigPath;
    public final Path nodeIdPath;
    public final Path clusterNamePath;


    public ConfigDirectory(Path baseConfigPath) throws Exception {
        this.baseConfigPath = baseConfigPath;
        this.nodeIdPath = baseConfigPath.resolve(nodeIdFile);
        this.clusterNamePath = baseConfigPath.resolve(clusterNameFile);

        init();
    }

    private void init() throws Exception {

        if (Files.exists(baseConfigPath) && !Files.isDirectory(baseConfigPath)) {
            throw new Exception("Base config path exists and is not a directory " + baseConfigPath);
        }

        if (!Files.exists(baseConfigPath)) {
            Files.createDirectories(baseConfigPath);
        }


        if (Files.exists(nodeIdPath) && !Files.isRegularFile(nodeIdPath)) {
            throw new Exception("NodeId file is not a regular directory!");
        }

        if (Files.exists(clusterNamePath) && !Files.isRegularFile(clusterNamePath)) {
            throw new Exception("Cluster name is not a regular directory!");
        }

        if (!Files.isWritable(baseConfigPath)) {
            throw new Exception("Can't write to the base configuration path!");
        }
    }

    /** Get the contents of the node id config file */
    public String getNodeId() throws IOException {
        return getFirstLineOfFile(nodeIdPath);
    }

    public String getClusterName() throws IOException {
        return getFirstLineOfFile(clusterNamePath);
    }

    public void createSubDir(String subDir) throws Exception {
        Path dirPath = baseConfigPath.resolve(subDir);
        if (Files.isRegularFile(dirPath) || !Files.isWritable(dirPath)) {
            throw new Exception("subdir isnt a dir or isnt writable!");
        }

        if (Files.isDirectory(dirPath)) {
            return;
        }
        Files.createDirectory(dirPath);
    }

    public void writeFile(String subDir, String fileName, List<String> data) throws IOException {
        Path filePath = baseConfigPath.resolve(subDir).resolve(fileName);
        Files.write(filePath, data, UTF_8);
    }

    public List<String> readFile(String subDir, String fileName) throws IOException {
        Path filePath = baseConfigPath.resolve(subDir).resolve(fileName);
        return Files.readAllLines(filePath, UTF_8);
    }

    private String getFirstLineOfFile(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            List<String> allLines = Files.readAllLines(path, UTF_8);
            if (allLines.isEmpty())
                return null;
            return allLines.get(0);
        }
        return null;
    }

    public void setNodeIdFile(String data) throws IOException {
        setFile(data, nodeIdPath);
    }
    public void setClusterNameFile(String data) throws IOException {
        setFile(data, clusterNamePath);
    }

    private void setFile(String data, Path path) throws IOException {
        List<String> lines = new ArrayList<>(1);
        lines.add(data);
        Files.write(path, lines, UTF_8);
    }
}
