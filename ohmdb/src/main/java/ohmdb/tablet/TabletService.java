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
package ohmdb.tablet;

import com.google.common.util.concurrent.AbstractService;
import ohmdb.interfaces.TabletModule;
import ohmdb.messages.ControlMessages;
import org.jetlang.channels.Channel;
import org.jetlang.channels.MemoryChannel;
import org.jetlang.fibers.Fiber;
import org.jetlang.fibers.PoolFiberFactory;

/**
 *
 */
public class TabletService extends AbstractService implements TabletModule {
    private final PoolFiberFactory fiberFactory;
    private final Fiber fiber;

    public TabletService(PoolFiberFactory fiberFactory) {
        this.fiberFactory = fiberFactory;
        this.fiber = fiberFactory.create();
    }

    @Override
    protected void doStart() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void doStop() {
        //To change body of implemented methods use File | Settings | File Templates.
    }



    private final Channel<TabletStateChange> tabletStateChangeChannel = new MemoryChannel<>();

    @Override
    public Channel<TabletStateChange> getTabletStateChanges() {
        return tabletStateChangeChannel;
    }

    @Override
    public ControlMessages.ModuleType getModuleType() {
        return ControlMessages.ModuleType.Tablet;
    }

    @Override
    public boolean hasPort() {
        return true;
    }

    @Override
    public int port() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
