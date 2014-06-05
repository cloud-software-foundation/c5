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
package c5db.client.codec.websocket;

import c5db.client.generated.Response;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.protostuff.ByteBufferInput;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WebsocketProtostuffDecoder extends WebSocketClientProtocolHandler {

  private static final long HANDSHAKE_TIMEOUT = 4000;
  private final WebSocketClientHandshaker handShaker;
  private final SettableFuture<Boolean> handshakeFuture = SettableFuture.create();

  public WebsocketProtostuffDecoder(WebSocketClientHandshaker handShaker) {
    super(handShaker);
    this.handShaker = handShaker;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof ClientHandshakeStateEvent) {
      final ClientHandshakeStateEvent clientHandshakeStateEvent = (ClientHandshakeStateEvent) evt;
      if (clientHandshakeStateEvent.equals(ClientHandshakeStateEvent.HANDSHAKE_COMPLETE)) {
        handshakeFuture.set(true);
      }
    }
    super.userEventTriggered(ctx, evt);
  }


  @Override
  protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
    if (frame instanceof BinaryWebSocketFrame) {
      final ByteBufferInput input = new ByteBufferInput(frame.content().nioBuffer(), false);
      final Response newMsg = Response.getSchema().newMessage();
      Response.getSchema().mergeFrom(input, newMsg);
      out.add(newMsg);
    } else {
      super.decode(ctx, frame, out);
    }
  }

  public void syncOnHandshake() throws InterruptedException, ExecutionException, TimeoutException {
    while (!this.handShaker.isHandshakeComplete()) {
      handshakeFuture.get(HANDSHAKE_TIMEOUT, TimeUnit.MILLISECONDS);
    }
  }
}
