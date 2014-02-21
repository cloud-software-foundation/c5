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
package c5db.codec;

import c5db.client.generated.ClientProtos;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyLiteralByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketServerHandshakerFactory;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaders.Names.HOST;
import static io.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static io.netty.handler.codec.http.HttpHeaders.setContentLength;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class WebsocketProtostuffDecoder extends MessageToMessageDecoder<Object> {

  private static final Logger logger = Logger.getLogger(WebsocketProtostuffDecoder.class.getName());
  private final String websocketPath;

  private WebSocketServerHandshaker handshaker;

  public WebsocketProtostuffDecoder(String websocketPath) {
    this.websocketPath = websocketPath;
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }


  @Override
  protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    if (msg instanceof FullHttpRequest) {
      handleHttpRequest(ctx, (FullHttpRequest) msg);
    } else if (msg instanceof WebSocketFrame) {
      handleWebSocketFrame(ctx, (WebSocketFrame) msg, out);
    }
  }

  private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws InvalidProtocolBufferException {
    // Check for closing frame
    if (frame instanceof CloseWebSocketFrame) {
      handshaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
      return;
    }
    if (frame instanceof PingWebSocketFrame) {
      ctx.channel().write(new PongWebSocketFrame(frame.content().retain()));
      return;
    }
    if (!(frame instanceof BinaryWebSocketFrame)) {
      throw new UnsupportedOperationException(String.format("%s frame types not supported", frame.getClass()
          .getName()));
    }
    out.add(ClientProtos.Call.parseFrom(ZeroCopyLiteralByteString.copyFrom(frame.content().nioBuffer())));

  }

  private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
    // Handle a bad request.
    if (!req.getDecoderResult().isSuccess()) {
      sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
      return;
    }

    // Allow only GET methods.
    if (req.getMethod() != GET) {
      sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, FORBIDDEN));
      return;
    }

    // Send the demo page and favicon.ico
    if ("/".equals(req.getUri())) {
      ByteBuf content = WebSocketServerIndexPage.getContent(getWebSocketLocation(req));
      FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);

      res.headers().set(CONTENT_TYPE, "text/html; charset=UTF-8");
      setContentLength(res, content.readableBytes());

      sendHttpResponse(ctx, req, res);
      return;
    }
    if ("/favicon.ico".equals(req.getUri())) {
      FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND);
      sendHttpResponse(ctx, req, res);
      return;
    }

    // Handshake
    WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(
        getWebSocketLocation(req), null, false);
    handshaker = wsFactory.newHandshaker(req);
    if (handshaker == null) {
      WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
    } else {
      handshaker.handshake(ctx.channel(), req);
    }
  }


  private static void sendHttpResponse(
      ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
    // Generate an error page if response getStatus code is not OK (200).
    if (res.getStatus().code() != 200) {
      ByteBuf buf = Unpooled.copiedBuffer(res.getStatus().toString(), CharsetUtil.UTF_8);
      res.content().writeBytes(buf);
      buf.release();
      setContentLength(res, res.content().readableBytes());
    }

    // Send the response and close the connection if necessary.
    ChannelFuture f = ctx.channel().writeAndFlush(res);
    if (!isKeepAlive(req) || res.getStatus().code() != 200) {
      f.addListener(ChannelFutureListener.CLOSE);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }

  private String getWebSocketLocation(FullHttpRequest req) {
    return "ws://" + req.headers().get(HOST) + this.websocketPath;
  }

}
