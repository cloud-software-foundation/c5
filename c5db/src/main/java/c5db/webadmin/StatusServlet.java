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

import c5db.interfaces.C5Module;
import c5db.interfaces.C5Server;
import c5db.interfaces.DiscoveryModule;
import c5db.interfaces.TabletModule;
import c5db.interfaces.discovery.NodeInfo;
import c5db.interfaces.tablet.Tablet;
import c5db.messages.generated.ModuleType;
import com.github.mustachejava.Mustache;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Handles the base page display
 */
@WebServlet(urlPatterns = {"/"})
public class StatusServlet extends HttpServlet {
  private WebAdminService service;

  public StatusServlet(WebAdminService service) {
    this.service = service;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
    try {
      response.setContentType("text/html");
      response.setStatus(HttpServletResponse.SC_OK);

      Mustache template = service.getMustacheFactory().compile("templates/index.mustache");
      Writer writer = response.getWriter();

      // Collect server status objects from around this place.
      ImmutableMap<ModuleType, C5Module> modules = service.getServer().getModules();

      ImmutableMap<Long, NodeInfo> nodes = getNodes();
      Collection<Tablet> tablets = getTablets();

      TopLevelHolder templateContext = new TopLevelHolder(service.getServer(), modules, nodes, tablets);
      template.execute(writer, templateContext);
      writer.flush();

    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      throw new IOException("Getting status", e);
    }
  }

  private Collection<Tablet> getTablets() throws ExecutionException, InterruptedException {
    TabletModule tabletModule = service.getTabletModule();
    if (tabletModule == null) {
      return null;
    }
    return tabletModule.getTablets();
  }

  private ImmutableMap<Long, NodeInfo> getNodes() throws InterruptedException, ExecutionException {
    DiscoveryModule discoveryModule = service.getDiscoveryModule();
    if (discoveryModule == null) {
      return null;
    }

    ListenableFuture<ImmutableMap<Long, NodeInfo>> nodeFuture =
        discoveryModule.getState();
    return nodeFuture.get();
  }


  private static class TopLevelHolder {
    public final C5Server server;
    private final Map<ModuleType, C5Module> modules;
    private final ImmutableMap<Long, NodeInfo> nodes;
    public final Collection<Tablet> tablets;

    private TopLevelHolder(C5Server server,
                           ImmutableMap<ModuleType, C5Module> modules,
                           ImmutableMap<Long, NodeInfo> nodes, Collection<Tablet> tablets) {
      this.server = server;
      this.modules = modules;
      this.nodes = nodes;
      this.tablets = tablets;
    }
    public Collection<Map.Entry<ModuleType, C5Module>> getModules() {
      if (modules == null) return null;
      return modules.entrySet();
    }
    public Collection<NodeInfo> getNodes() {
      if (nodes == null) return null;
      return nodes.values();
    }
  }
}

