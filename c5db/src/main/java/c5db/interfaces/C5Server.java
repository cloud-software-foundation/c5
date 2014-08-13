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

package c5db.interfaces;

import c5db.ConfigDirectory;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.interfaces.server.ConfigKeyUpdated;
import c5db.messages.generated.CommandReply;
import c5db.util.FiberSupplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.jetlang.channels.Channel;
import org.jetlang.channels.RequestChannel;

/**
 * A C5Server stands in for global resources that modules might need.  It provides global
 * services, configuration, notification buses and more.
 * <p/>
 * Right now this interface is a little too kitchen-sinky, and it should probably have
 * individual responsibilities broken off to make dependencies a bit more clear.
 * <p/>
 * Note that multiple {@link c5db.interfaces.C5Server} may be in a single JVM, so avoiding
 * static method calls is of paramount importance.
 */
public interface C5Server extends ModuleServer, Service {
  /**
   * Every server has a persistent id that is independent of it's (in some cases temporary)
   * network or host identification.  Normally this would be persisted in a configuration
   * file, and generated randomly (64 bits is enough for everyone, right?).  There may be
   * provisions to allow administrators to assign node ids.
   * <p/>
   *
   * @return THE node id for this server.
   */
  public long getNodeId();

  /**
   * A jetlang channel to submit command objects to the 'system'.
   * A command is the extensible (and not entirely specified/thought out) mechanism by which
   * entities (code, RPCs, people via command line interfaces, etc) can start/stop/adjust
   * modules or other configuration settings, or anything else at a cross-service level.
   * <p/>
   * The intent is to centralize and standardize all configuration, startup, and other details
   * on how we manage services, servers and whatnot.  Every single operation should be accessible
   * via command messages, and those command messages should be able to be conveyed via RPC.
   * This allows administration-via tooling, and avoids the need to have something like
   * password-less ssh set up (which not all wish to do).
   * <p/>
   *
   * @return The jetlang channel to submit command messages
   */
  public Channel<CommandRpcRequest<?>> getCommandChannel();

  /**
   * Similar to {@link #getCommandChannel()} except providing a feedback message with information
   * on the status and success of commands.
   * <p/>
   *
   * @return The jetlang request channel to submit requests
   */
  public RequestChannel<CommandRpcRequest<?>, CommandReply> getCommandRequests();

  public ConfigDirectory getConfigDirectory();

  public boolean isSingleNodeMode();

  /**
   * Return a FiberSupplier with which the caller may create a Fiber using the server's
   * resources (for example, a shared fiber pool).
   */
  FiberSupplier getFiberSupplier();

  ListenableFuture<Void> getShutdownFuture();

  int getMinQuorumSize();

  public Channel<ConfigKeyUpdated> getConfigUpdateChannel();
}
