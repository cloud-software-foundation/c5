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
package c5db.module_cfg;

import c5db.messages.generated.ModuleType;
import c5db.util.Graph;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;


/**
 *
 */
public class ModuleDepsTest {

  @Test
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

    List<ImmutableList<Graph.Node<ModuleType>>> result = ModuleDeps.createGraph("c5db.interfaces.RegionServerModule");

    Joiner joiner = Joiner.on("\n");

    System.out.println(joiner.join(result));
  }
}
