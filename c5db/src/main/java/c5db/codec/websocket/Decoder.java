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

package c5db.codec.websocket;

import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.util.List;


/**
 * A specialized Protostuff decoder used to deserialize Protostuff from a WebSocketStream and map them to a Call
 * object.
 */
public class Decoder extends WebSocketServerProtocolHandler {
  CompositeByteBuf previousFrameData = null;

  public Decoder(String websocketPath) {
    super(websocketPath);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
    try {
      if (frame instanceof BinaryWebSocketFrame || frame instanceof ContinuationWebSocketFrame) {
        handleC5Frame(frame, out);
      } else {
        super.decode(ctx, frame, out);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void handleC5Frame(WebSocketFrame frame, List<Object> out) {
    if (previousFrameData == null && frame.isFinalFragment()) {
      out.add(frame.content().retain());
    } else { // We have a stream of frames comming in
      if (previousFrameData == null) { // first in a series
        previousFrameData = Unpooled.compositeBuffer();
      }

      previousFrameData.addComponent(frame.content().retain());
      previousFrameData.writerIndex(previousFrameData.writerIndex() + frame.content().readableBytes());

      if (frame.isFinalFragment()) { // last in a series
        out.add(previousFrameData);
        previousFrameData = null;
      }
    }
  }
}