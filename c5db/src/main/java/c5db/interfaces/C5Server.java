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

import c5db.NioFileConfigDirectory;
import c5db.messages.generated.CommandReply;
import c5db.messages.generated.ModuleType;
import com.dyuproject.protostuff.Message;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.jetlang.channels.Channel;
import org.jetlang.channels.RequestChannel;

import java.util.concurrent.ExecutionException;

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
public interface C5Server extends Service {
  /**
   * Every server has a persistent id that is independent of it's (in some cases temporary)
   * network or host identification.  Normally this would be persisted in a configuration
   * file, and generated randomly (64 bits is enough for everyone, right?).  There may be
   * provisions to allow adminstrators to assign node ids.
   * <p>
   * @return THE node id for this server.
   */
    public long getNodeId();

    // TODO this could be generified if we used an interface instead of ModuleType

  /**
   * This is primary mechanism via which modules with compile time binding via interfaces
   * that live in {@link c5db.interfaces} may obtain instances of their dependencies.
   * <p>
   * This method returns a future, which implies that the module may not be started yet.
   * The future will be signalled when the module is started, and callers may just add a
   * callback and wait.
   * <p>
   * In the future when automatic service startup order is working, this method might just
   * return the type without a future, or may not require much/any waiting.
   * <p>
   * Right now modules are specified via an enum, in the future perhaps we should
   * use a Java interface type?
   * @param moduleType the specific module type you wish to retrieve
   * @return a future that will be set when the module is running
   */
  public ListenableFuture<C5Module> getModule(ModuleType moduleType);

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
   * @return The jetlang channel to submit command messages
   */
  public Channel<Message<?>> getCommandChannel();

  /**
   * Similar to {@link #getCommandChannel()} except providing a feedback message with information
   * on the status and success of commands.
   * <p>
   * @return The jetlang request channel to submit requests
   */
  public RequestChannel<Message<?>, CommandReply> getCommandRequests();

    public Channel<ModuleStateChange> getModuleStateChangeChannel();

    public ImmutableMap<ModuleType, C5Module> getModules() throws ExecutionException, InterruptedException;

    public ListenableFuture<ImmutableMap<ModuleType, C5Module>> getModules2();

    public NioFileConfigDirectory getConfigDirectory();

    public boolean isSingleNodeMode();

    public static class ModuleStateChange {
        public final C5Module module;
        public final State state;

        @Override
        public String toString() {
            return "ModuleStateChange{" +
                    "module=" + module +
                    ", state=" + state +
                    '}';
        }

        public ModuleStateChange(C5Module module, State state) {
            this.module = module;
            this.state = state;
        }
    }

    public Channel<ConfigKeyUpdated> getConfigUpdateChannel();

    public static class ConfigKeyUpdated {
        public final String configKey;
        public final Object configValue;

        public ConfigKeyUpdated(String configKey, Object configValue) {
            this.configKey = configKey;
            this.configValue = configValue;
        }

        @Override
        public String toString() {
            return "ConfigKeyUpdated{" +
                    "configKey='" + configKey + '\'' +
                    ", configValue=" + configValue +
                    '}';
        }

    }
}
