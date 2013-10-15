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
    }

    /** Get the contents of the node id config file */
    public String getNodeId() throws IOException {
        return getFirstLineOfFile(nodeIdPath);
    }

    public String getClusterName() throws IOException {
        return getFirstLineOfFile(clusterNamePath);
    }

    private String getFirstLineOfFile(Path path) throws IOException {
        if (Files.isRegularFile(path)) {
            List<String> allLines = Files.readAllLines(path, Charset.forName("UTF-8"));
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
        Files.write(path, lines, Charset.forName("UTF-8"));
    }
}
