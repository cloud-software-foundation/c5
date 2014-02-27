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
import com.google.protobuf.ZeroCopyLiteralByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.util.List;

public class WebsocketProtostuffDecoder extends WebSocketServerProtocolHandler {

  public WebsocketProtostuffDecoder(String websocketPath) {
    super(websocketPath);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
    if (frame instanceof BinaryWebSocketFrame) {
      out.add(ClientProtos.Call.parseFrom(ZeroCopyLiteralByteString.copyFrom(frame.content().nioBuffer())));
    }
    super.decode(ctx, frame, out);
  }
}
