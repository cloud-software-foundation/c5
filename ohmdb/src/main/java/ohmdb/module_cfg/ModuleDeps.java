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
package ohmdb.module_cfg;

import com.google.common.collect.ImmutableList;
import ohmdb.discovery.BeaconService;
import ohmdb.interfaces.DiscoveryModule;
import ohmdb.interfaces.OhmModule;
import ohmdb.interfaces.ReplicationModule;
import ohmdb.interfaces.TabletModule;
import ohmdb.replication.ReplicatorService;
import ohmdb.tablet.TabletService;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static ohmdb.messages.ControlMessages.ModuleType;

/**
 *
 */
public class ModuleDeps {

    public static void doTarjan(ImmutableList<ModuleType> seed) {
        Map<ModuleType, Node> allNodes = buildDepGraph(seed);

        resetIndexes(allNodes.values());

        int index = 0;
        Deque<Node> s = new LinkedList<>();
        List<ImmutableList<Node>> components = new LinkedList<>();
        for (Node v : allNodes.values()) {

            index = strongConnect(index, s, v, components);
        }


    }

    private static int strongConnect(int index, Deque<Node> s, Node v, List<ImmutableList<Node>> components) {
        if (v.index == -1) {
            v.index = index;
            v.lowlink = index;

            index += 1;

            s.push(v);

            for ( Node w : v.dependencies) {
                if (w.index == -1) {
                    index = strongConnect(index, s, w, components);

                    v.lowlink = Math.min(v.lowlink, w.lowlink);

                } else if (s.contains(w)) {
                    v.lowlink = Math.min(v.lowlink, w.index);
                }
            }

            if (v.lowlink == v.index) {
                List<Node> component = new LinkedList<>();
                Node w = null;
//                while ( (w = s.pop()) != v) {
//                    component.add(w);
//                }
                do {
                    w = s.pop();
                    component.add(w);
                } while (w != v);
                System.out.println("Component: " + component);
                components.add(ImmutableList.copyOf(component));
            }

        }
        return index;
    }

    private static void resetIndexes(Collection<Node> values) {
        for (Node n : values) {
            n.index = -1;
            n.lowlink = -1;
        }
    }


    public static Map<ModuleType, Node> buildDepGraph(ImmutableList<ModuleType> seed) {
        Map<ModuleType, Node> nodes = new HashMap<>();
        Set<ModuleType> processed = new HashSet<>();
        Queue<ModuleType> q = new LinkedList<>();
        q.addAll(seed);

        ModuleType parent = null;
        while ((parent = q.poll()) != null) {
            if (processed.contains(parent))
                continue;
            else
                processed.add(parent);

            if (!nodes.containsKey(parent))
                nodes.put(parent, new Node(parent));

            ImmutableList<ModuleType> mdeps = getDependency(parent);
            q.addAll(mdeps);
            for (ModuleType dependent : mdeps) {
                Node pNode = nodes.get(parent);
                Node dNode = null;
                if (nodes.containsKey(dependent)) {
                    dNode = nodes.get(dependent);
                } else {
                    dNode = new Node(dependent);
                    nodes.put(dependent, dNode);
                }

                pNode.dependencies.add(dNode);
            }
        }
        return nodes;
    }

    public static ImmutableList<ModuleType> getDependency(ModuleType moduleType) {
        switch (moduleType) {
            case Discovery:
                return ImmutableList.of(ModuleType.Services);
            case Replication:
                return ImmutableList.of(ModuleType.Discovery);
            case Tablet:
                return ImmutableList.of(ModuleType.Replication);
            case Client:
                return ImmutableList.of(ModuleType.Tablet, ModuleType.Management);
            case Management:
                return ImmutableList.of(ModuleType.Tablet, ModuleType.Replication);
            case Services:
                return ImmutableList.of();
            default:
                throw new RuntimeException("Someone forgot to extend this switch statement");

        }
    }

    public static Class<? extends OhmModule> getImplClass(ModuleType moduleType) {
        switch (moduleType) {
            case Discovery:
                return BeaconService.class;
            case Replication:
                return ReplicatorService.class;
            case Tablet:
                return TabletService.class;
            default:
                throw new RuntimeException("Someone forgot to extend this switch statement");

        }
    }

    public static Class<? extends OhmModule> getInterface(ModuleType moduleType) {
        switch (moduleType) {
            case Discovery:
                return DiscoveryModule.class;
            case Replication:
                return ReplicationModule.class;
            case Tablet:
                return TabletModule.class;
            default:
                throw new RuntimeException("Someone forgot to extend this switch statement");
        }
    }

    private static class Node {
        public final Set<Node> dependencies = new HashSet<>();
        public final ModuleType type;
        public int index = -1;
        public int lowlink = -1;

        private Node(ModuleType type) {
            this.type = type;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "type=" + type +
                    ", dependencies=" + dependencies.size() +
                    ", index,lowlink=" + index + "," + lowlink +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Node node = (Node) o;

            if (type != node.type) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return type.hashCode();
        }
    }
}
