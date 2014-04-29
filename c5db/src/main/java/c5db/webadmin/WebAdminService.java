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

import c5db.interfaces.C5Server;
import c5db.interfaces.WebAdminModule;
import c5db.messages.generated.ModuleType;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.MustacheFactory;
import com.google.common.util.concurrent.AbstractService;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;

import java.net.URL;

/**
 *
 */
public class WebAdminService extends AbstractService implements WebAdminModule {

  private final C5Server server;
  private final int port;
  private Server jettyHttpServer;

  public WebAdminService(C5Server server,
                         int port) {
    this.server = server;
    this.port = port;
  }

  public MustacheFactory getMustacheFactory() {
    // TODO return a reused one if in production mode.
    return new DefaultMustacheFactory();
  }

  @Override
  protected void doStart() {
    // TODO do this in a thread or whatever
    jettyHttpServer = new Server(port);

    try {
      URL webResourcesUrl = getClass().getClassLoader().getResource("web");
      ResourceHandler resources = new ResourceHandler();
      resources.setDirectoriesListed(false);

      resources.setBaseResource(Resource.newResource(webResourcesUrl));

      HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{
          new StatusHandler(this),
          resources,
          new DefaultHandler()
      });

      jettyHttpServer.setHandler(handlerList);

      jettyHttpServer.start();

    } catch (Exception e) {
      notifyFailed(e);
    }
    notifyStarted();
  }

  @Override
  protected void doStop() {
    try {
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
