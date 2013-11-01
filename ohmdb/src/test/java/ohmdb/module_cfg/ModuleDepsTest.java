package ohmdb.module_cfg;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static ohmdb.messages.ControlMessages.ModuleType;

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

        ModuleDeps.doTarjan(ImmutableList.of(ModuleType.Management, ModuleType.Client, ModuleType.Discovery));
    }
}
