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

package c5db.module_cfg;

import c5db.discovery.BeaconService;
import c5db.interfaces.C5Module;
import c5db.interfaces.DependsOn;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.ModuleTypeBinding;
import c5db.interfaces.ReplicationModule;
import c5db.interfaces.TabletModule;
import c5db.messages.generated.ModuleType;
import c5db.replication.ReplicatorService;
import c5db.tablet.TabletService;
import c5db.util.Graph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 *
 */
public class ModuleDeps {
  private static final Logger LOG = LoggerFactory.getLogger(ModuleDeps.class);

  public static List<ModuleType> getModuleReverseDependencyOrder(Collection<Class<?>> startThese)
      throws ClassNotFoundException {
    List<ModuleType> moduleTypeList = new ArrayList<>(startThese.size());

    for (Graph.Node<ModuleType> node : Iterables.concat(createGraph(startThese))) {
      moduleTypeList.add(node.type);
    }

    return moduleTypeList;
  }

  public static List<ImmutableList<Graph.Node<ModuleType>>> createGraph(String... startThese) throws ClassNotFoundException {
    List<Class<?>> classList = new LinkedList<>();

    for (String name : startThese) {
      Class<?> c = Class.forName(name);
      classList.add(c);
    }

    return createGraph(classList);
  }

  public static List<ImmutableList<Graph.Node<ModuleType>>> createGraph(Collection<Class<?>> startThese)
      throws ClassNotFoundException {
    Map<ModuleType, Graph.Node<ModuleType>> allNodes = new HashMap<>();
    Queue<Class<?>> queue = new LinkedList<>(startThese);

    Class<?> parent;
    while ((parent = queue.poll()) != null) {
      ModuleTypeBinding mt = parent.getAnnotation(ModuleTypeBinding.class);

      if (mt == null) {
        LOG.warn("Module interface {} has no type annotation - skipping", parent);
        continue;
      }

      ModuleType type = mt.value();
      Graph.Node<ModuleType> node = allNodes.get(type);
      if (node == null) {
        node = new Graph.Node<>(type);
        allNodes.put(type, node);
      }

      DependsOn deps = parent.getAnnotation(DependsOn.class);
      if (deps == null) {
        continue;
      }

      for (Class<?> dep : deps.value()) {
        // a direct dependency:
        ModuleTypeBinding depMT = dep.getAnnotation(ModuleTypeBinding.class);
        if (depMT == null) {
          LOG.warn("Module {} depends on {} which has no type", parent, dep);
          continue; // this really shouldnt happen.
        }

        Graph.Node<ModuleType> depNode = allNodes.get(depMT.value());
        if (depNode == null) {
          depNode = new Graph.Node<>(depMT.value());
          allNodes.put(depMT.value(), depNode);
        }
        node.dependencies.add(depNode);
        // add to the queue:
        queue.add(dep);
      }
    }

    //Joiner joiner = Joiner.on("\n");
    //System.out.println(joiner.join(typeClassMap.entrySet()));
    return Graph.doTarjan(allNodes.values());
  }

  public static void doTarjan(ImmutableList<ModuleType> seed) {

    Map<ModuleType, Graph.Node<ModuleType>> allNodes = buildDepGraph(seed);

    Graph.doTarjan(allNodes.values());
  }


  public static Map<ModuleType, Graph.Node<ModuleType>> buildDepGraph(ImmutableList<ModuleType> seed) {
    Map<ModuleType, Graph.Node<ModuleType>> nodes = new HashMap<>();
    Set<ModuleType> processed = new HashSet<>();
    Queue<ModuleType> q = new LinkedList<>();
    q.addAll(seed);

    ModuleType parent;
    while ((parent = q.poll()) != null) {
      if (processed.contains(parent)) {
        continue;
      } else {
        processed.add(parent);
      }

      if (!nodes.containsKey(parent)) {
        nodes.put(parent, new Graph.Node<>(parent));
      }

      ImmutableList<ModuleType> mdeps = getDependency(parent);
      q.addAll(mdeps);
      for (ModuleType dependent : mdeps) {
        Graph.Node<ModuleType> pNode = nodes.get(parent);
        Graph.Node<ModuleType> dNode;
        if (nodes.containsKey(dependent)) {
          dNode = nodes.get(dependent);
        } else {
          dNode = new Graph.Node<>(dependent);
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
        return ImmutableList.of();
      case Replication:
        return ImmutableList.of(ModuleType.Discovery);
      case Tablet:
        return ImmutableList.of(ModuleType.Replication);
      case RegionServer:
        return ImmutableList.of(ModuleType.Tablet, ModuleType.Management);
      case Management:
        return ImmutableList.of(ModuleType.Tablet, ModuleType.Replication);
      default:
        throw new RuntimeException("Someone forgot to extend this switch statement");

    }
  }

  public static Class<? extends C5Module> getImplClass(ModuleType moduleType) {
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

  public static Class<? extends C5Module> getInterface(ModuleType moduleType) {
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

}
