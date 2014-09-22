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

import c5db.messages.generated.ModuleType;
import c5db.util.Graph;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

public class ModuleDepsTest {

  //@Test
  public void testTrans() throws Exception {
//        ImmutableList<ImmutableList<ModuleType>> lst = ModuleDeps.getTransDeps(ImmutableList.of(ModuleType.Management, ModuleType.Client, ModuleType.Discovery));
//        for (ImmutableList<ModuleType> l : lst) {
//            System.out.println(l);
//        }
//        System.out.println(lst);

    ModuleDeps.doTarjan(ImmutableList.of(ModuleType.Management, ModuleType.RegionServer, ModuleType.Discovery));


  }

  @Test
  public void testFoo() throws Exception {

    List<ImmutableList<Graph.Node<ModuleType>>> result = ModuleDeps.createGraph("c5db.interfaces.RegionServerModule", "c5db.interfaces.EventLogModule");

    Joiner joiner = Joiner.on("\n");

    System.out.println("result joined:");
    System.out.println(joiner.join(result));
  }
}
