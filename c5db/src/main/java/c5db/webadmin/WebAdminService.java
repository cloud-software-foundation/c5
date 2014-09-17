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

package c5db.webadmin;

import c5db.discovery.generated.Availability;
import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.WebAdminModule;
import c5db.interfaces.discovery.NewNodeVisible;
import c5db.interfaces.tablet.TabletStateChange;
import c5db.messages.generated.ModuleType;
import c5db.util.C5Futures;
import c5db.util.FiberOnly;
import c5db.webadmin.generated.TabletStateNotification;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ListenableFuture;
import io.protostuff.JsonIOUtil;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.jetlang.fibers.Fiber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;

/**
 *
 */
public class WebAdminService extends AbstractService implements WebAdminModule {
  private static final Logger LOG = LoggerFactory.getLogger(WebAdminService.class);

  private final C5Server server;
  private final int port;
  private final Fiber fiber;
  private Server jettyHttpServer;

  private DiscoveryModule discoveryModule = null;
  private TabletModule tabletModule = null;

  public WebAdminService(C5Server server, int port) {
    this.server = server;
    this.port = port;
    this.fiber = server.getFiberSupplier().getFiber(this::handleThrowable);
  }

  private void handleThrowable(Throwable fiberError) {
    // TODO do nothing for now!
    LOG.error("Got fiber exception", fiberError);
  }

  public MustacheFactory getMustacheFactory() {
    // TODO return a reused one if in production mode.
    return new DefaultMustacheFactory();
  }

  public TabletModule getTabletModule() {
    return tabletModule;
  }

  public DiscoveryModule getDiscoveryModule() {
    return discoveryModule;
  }

  @Override
  protected void doStart() {
    fiber.start();
    fiber.execute(() -> {
      ListenableFuture<C5Module> future = server.getModule(ModuleType.Discovery);
      C5Futures.addCallback(future, module -> {
        discoveryModule = (DiscoveryModule) module;

        discoveryModule.getNewNodeNotifications().subscribe(fiber, this::newNodeVisible);
      }, this::handleThrowable, fiber);
    });
    fiber.execute(() -> {
      ListenableFuture<C5Module> future = server.getModule(ModuleType.Tablet);
      C5Futures.addCallback(future, module -> {
        tabletModule = (TabletModule) module;

        tabletModule.getTabletStateChanges().subscribe(fiber, this::tabletStateChanges);
      }, this::handleThrowable, fiber);
    });

    // TODO do this in a thread or whatever
    jettyHttpServer = new Server(port);

    try {
      ServletContextHandler indexServlet = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      indexServlet.setContextPath("/index");
      indexServlet.setAllowNullPathInfo(true);
      indexServlet.addServlet(new ServletHolder(new StatusServlet(this)), "/");

      ServletContextHandler pushServlet = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      pushServlet.setContextPath("/push");
      pushServlet.addServlet(new ServletHolder(new WebsocketServlet(this)), "/");

      ServletContextHandler createTableServlet = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      createTableServlet.setContextPath("/create-table");
      createTableServlet.addServlet(new ServletHolder(new CreateTableServlet(server)), "/");

      ContextHandlerCollection servletContexts = new ContextHandlerCollection();
      servletContexts.addHandler(indexServlet);
      servletContexts.addHandler(pushServlet);
      servletContexts.addHandler(createTableServlet);

      URL webResourcesUrl = getClass().getClassLoader().getResource("web");
      ResourceHandler resources = new ResourceHandler();
      resources.setDirectoriesListed(false);
      resources.setBaseResource(Resource.newResource(webResourcesUrl));

      HandlerList topHandlerList = new HandlerList();
      topHandlerList.setHandlers(new Handler[]{
          servletContexts,
          resources
      });

      RewriteHandler rewriteRoot = new RewriteHandler();
      rewriteRoot.setRewriteRequestURI(true);
      rewriteRoot.setRewritePathInfo(true);
      rewriteRoot.setOriginalPathAttribute("requestedPath");
      RewriteRegexRule rule = new RewriteRegexRule();
      rule.setRegex("/");
      rule.setReplacement("/index");
      rewriteRoot.addRule(rule);
      rewriteRoot.setHandler(topHandlerList);

      jettyHttpServer.setHandler(rewriteRoot);
      jettyHttpServer.start();
    } catch (Exception e) {
      notifyFailed(e);
    }
    notifyStarted();
  }

  public ConcurrentHashSet<WebsocketClient> websocketClients = new ConcurrentHashSet<>();

  @FiberOnly
  private void tabletStateChanges(TabletStateChange tabletStateChange) {
    // TODO send to the websockets
    TabletStateNotification note = new TabletStateNotification(
        tabletStateChange.tablet.getRegionInfo().getEncodedName(),
        tabletStateChange.tablet.getRegionInfo().getRegionNameAsString(),
        tabletStateChange.state.toString(),
        tabletStateChange.tablet.getPeers(),
        0
    );
    ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
    try {
      JsonIOUtil.writeTo(jsonBytes, note, note, false);

      String jsonString = jsonBytes.toString();
      String finalString = "{\"type\":\"tablet\", \"data\": " + jsonString + "}";
      System.out.println(finalString);

      for (WebsocketClient client : websocketClients) {
        RemoteEndpoint wsRemote = client.getRemote();
        if (wsRemote != null) {
          client.getRemote().sendString(finalString);
        }
      }
    } catch (IOException e) {
      LOG.error("Processing tablet message", e);
    }

  }

  @FiberOnly
  private void newNodeVisible(NewNodeVisible nodeVisibilityMessage) {
    Availability availability = nodeVisibilityMessage.nodeInfo.availability;
    ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
    try {
      JsonIOUtil.writeTo(jsonBytes, availability, availability, false);

      String jsonString = jsonBytes.toString();
      String finalString = "{\"type\":\"newNode\", \"data\": " + jsonString + "}";
      System.out.println(finalString);

      for (WebsocketClient client : websocketClients) {
        RemoteEndpoint wsRemote = client.getRemote();
        if (wsRemote != null) {
          client.getRemote().sendString(finalString);
        }
      }
    } catch (IOException e) {
      LOG.error("Processing new node visible message", e);
    }
  }

  @Override
  protected void doStop() {
    try {
      fiber.dispose();
      jettyHttpServer.stop();
    } catch (Exception e) {
      notifyFailed(e);
    }
    notifyStopped();
  }

  public C5Server getServer() {
    return this.server;
  }

  @Override
  public ModuleType getModuleType() {
    return ModuleType.WebAdmin;
  }

  @Override
  public boolean hasPort() {
    return true;
  }

  @Override
  public int port() {
    return port;
  }

  @Override
  public String acceptCommand(String commandString) {
    return null;
  }
}
