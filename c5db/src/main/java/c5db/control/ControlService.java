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
package c5db.control;

import com.google.common.util.concurrent.AbstractService;
import io.netty.channel.nio.NioEventLoopGroup;
import org.jetlang.fibers.Fiber;

/**
 * Starts a HTTP service to listen for and respond to control messages.
 */
public class ControlService extends AbstractService {

  private final Fiber serviceFiber;
  private final NioEventLoopGroup acceptConnectionGroup;
  private final NioEventLoopGroup ioWorkerGroup;
  private final int modulePort;

  public ControlService(Fiber serviceFiber,
                        NioEventLoopGroup acceptConnectionGroup,
                        NioEventLoopGroup ioWorkerGroup,
                        int modulePort) {
    this.serviceFiber = serviceFiber;
    this.acceptConnectionGroup = acceptConnectionGroup;
    this.ioWorkerGroup = ioWorkerGroup;
    this.modulePort = modulePort;
  }

  @Override
  protected void doStart() {
    notifyStarted();
  }

  @Override
  protected void doStop() {
    notifyStopped();
  }
}
