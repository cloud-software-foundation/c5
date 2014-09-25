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

package c5db.webadmin;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;

/**
 * A connected websocket is represented by this object.
 */
public class WebsocketClient extends WebSocketAdapter {
  private WebAdminService service;

  public WebsocketClient(WebAdminService service) {

    this.service = service;
  }

  @Override
  public void onWebSocketConnect(Session sess) {
    super.onWebSocketConnect(sess);

    service.websocketClients.add(this);
  }

  @Override
  public void onWebSocketClose(int statusCode, String reason) {
    super.onWebSocketClose(statusCode, reason);

    service.websocketClients.remove(this);
  }

  @Override
  public void onWebSocketError(Throwable cause) {
    super.onWebSocketError(cause);

    service.websocketClients.remove(this);
  }
}
