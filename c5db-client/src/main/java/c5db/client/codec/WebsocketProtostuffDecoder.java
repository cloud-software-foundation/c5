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
package c5db.client.codec;

import c5db.client.generated.ClientProtos;
import com.google.protobuf.ZeroCopyLiteralByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.CharsetUtil;

import java.net.URISyntaxException;
import java.util.List;


public class WebsocketProtostuffDecoder extends MessageToMessageDecoder<Object> {
  public final WebSocketClientHandshaker handshaker;
  public ChannelPromise handshakeFuture;

  public WebsocketProtostuffDecoder(WebSocketClientHandshaker handShaker) throws URISyntaxException {
    this.handshaker = handShaker;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    handshakeFuture = ctx.newPromise();
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    handshaker.handshake(ctx.channel());
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    System.out.println("WebSocket Client disconnected!");
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    Channel ch = ctx.channel();
    if (!handshaker.isHandshakeComplete()) {
      handshaker.finishHandshake(ch, (FullHttpResponse) msg);
      System.out.println("WebSocket Client connected!");
      handshakeFuture.setSuccess();
      return;
    }

    if (msg instanceof FullHttpResponse) {
      FullHttpResponse response = (FullHttpResponse) msg;
      throw new Exception("Unexpected FullHttpResponse (getStatus=" + response.getStatus() + ", content="
          + response.content().toString(CharsetUtil.UTF_8) + ')');
    }

    WebSocketFrame frame = (WebSocketFrame) msg;
    if (frame instanceof BinaryWebSocketFrame) {
      out.add(ClientProtos.Response.parseFrom(
          ZeroCopyLiteralByteString.copyFrom(frame.content().nioBuffer()))
      );
    } else if (frame instanceof PongWebSocketFrame) {
      System.out.println("WebSocket Client received pong");
    } else if (frame instanceof CloseWebSocketFrame) {
      System.out.println("WebSocket Client received closing");
      ch.close();
      ch.closeFuture().sync();
    } else {
      msg.toString();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();

    if (!handshakeFuture.isDone()) {
      handshakeFuture.setFailure(cause);
    }

    ctx.close();
  }

}
