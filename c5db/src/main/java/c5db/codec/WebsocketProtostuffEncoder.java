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

import c5db.C5ServerConstants;
import c5db.client.generated.Response;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.protostuff.LowCopyProtobufOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A specialized Protostuff encoder used to serialize Protostuff into a WebSocketStream and map them to a Response
 * object. Special care must be paid to handle chunking websocket files transparently for the user.
 */
public class WebsocketProtostuffEncoder extends MessageToMessageEncoder<Response> {

  private static final long MAX_SIZE = C5ServerConstants.MAX_CONTENT_LENGTH_HTTP_AGG;
  private static final Logger LOG = LoggerFactory.getLogger(WebsocketProtostuffEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        Response response,
                        List<Object> objects) throws IOException {
    final LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput();
    Response.getSchema().writeTo(lcpo, response);
    final long size = lcpo.buffer.size();

    ByteBuf byteBuf = channelHandlerContext.alloc().buffer((int) size);
    lcpo.buffer.finish().stream().forEach(byteBuf::writeBytes);

    if (size < MAX_SIZE) {
      final BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
      objects.add(frame);
    } else {
      long remaining = size;
      boolean first = true;
      while (remaining > 0) {
        WebSocketFrame frame;
        if (remaining > MAX_SIZE) {
          final ByteBuf slice = byteBuf.copy((int) (size - remaining), (int) MAX_SIZE);
          if (first) {
            frame = new BinaryWebSocketFrame(false, 0, slice);
            first = false;
          } else {
            frame = new ContinuationWebSocketFrame(false, 0, slice);
          }
          remaining -= MAX_SIZE;
        } else {
          frame = new ContinuationWebSocketFrame(true, 0, byteBuf.copy((int) (size - remaining), (int) remaining));
          remaining = 0;
        }
        objects.add(frame);
      }
    }
  }
}
