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
 * <p>
 * Right now this interface is a little too kitchen-sinky, and it should probably have
 * individual responsibilities broken off to make dependencies a bit more clear.
 * <p>
 * Note that multiple {@link c5db.interfaces.C5Server} may be in a single JVM, so avoiding
 * static method calls is of paramount importance.
 */
public interface C5Server extends ModuleInformationProvider, Service {
  /**
   * Every server has a persistent id that is independent of it's (in some cases temporary)
   * network or host identification.  Normally this would be persisted in a configuration
   * file, and generated randomly (64 bits is enough for everyone, right?).  There may be
   * provisions to allow administrators to assign node ids.
   * <p>
   *
   * @return THE node id for this server.
   */
  public long getNodeId();

  /**
   * A jetlang channel to submit command objects to the 'system'.
   * A command is the extensible (and not entirely specified/thought out) mechanism by which
   * entities (code, RPCs, people via command line interfaces, etc) can start/stop/adjust
   * modules or other configuration settings, or anything else at a cross-service level.
   * <p>
   * The intent is to centralize and standardize all configuration, startup, and other details
   * on how we manage services, servers and whatnot.  Every single operation should be accessible
   * via command messages, and those command messages should be able to be conveyed via RPC.
   * This allows administration-via tooling, and avoids the need to have something like
   * password-less ssh set up (which not all wish to do).
   * <p>
   *
   * @return The jetlang channel to submit command messages
   */
  public Channel<CommandRpcRequest<?>> getCommandChannel();

  /**
   * Similar to {@link #getCommandChannel()} except providing a feedback message with information
   * on the status and success of commands.
   * <p>
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
