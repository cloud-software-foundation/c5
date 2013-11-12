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
package ohmdb.regionserver;

import com.google.common.util.concurrent.AbstractService;
import io.netty.channel.nio.NioEventLoopGroup;
import ohmdb.interfaces.OhmServer;
import ohmdb.interfaces.RegionServerModule;
import ohmdb.messages.ControlMessages;
import org.jetlang.fibers.PoolFiberFactory;

/**
 *
 */
public class RegionServerService extends AbstractService implements RegionServerModule {
    private final PoolFiberFactory fiberFactory;
    private final NioEventLoopGroup acceptGroup;
    private final NioEventLoopGroup workerGroup;
    private final int port;
    private final OhmServer server;

    public RegionServerService(PoolFiberFactory fiberFactory,
                               NioEventLoopGroup acceptGroup,
                               NioEventLoopGroup workerGroup,
                               int port,
                               OhmServer server) {
        this.fiberFactory = fiberFactory;
        this.acceptGroup = acceptGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        this.server = server;
    }

    @Override
    protected void doStart() {

        // TODO stuff.
    }

    @Override
    protected void doStop() {

    }

    @Override
    public ControlMessages.ModuleType getModuleType() {
        return ControlMessages.ModuleType.RegionServer;
    }

    @Override
    public boolean hasPort() {
        return true;
    }

    @Override
    public int port() {
        return port;
    }
}
