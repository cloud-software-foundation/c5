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

import c5db.client.C5Constants;
import c5db.client.generated.Call;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.protostuff.LowCopyProtobufOutput;

import java.nio.ByteBuffer;
import java.util.List;

public class WebsocketProtostuffEncoder extends MessageToMessageEncoder<Call> {
  private static final long MAX_SIZE = C5Constants.MAX_CONTENT_LENGTH_HTTP_AGG;
  private final WebSocketClientHandshaker handShaker;

  public WebsocketProtostuffEncoder(WebSocketClientHandshaker handShaker) {
    this.handShaker = handShaker;
  }

  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        Call call,
                        List<Object> objects) throws Exception {

    final LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput();
    Call.getSchema().writeTo(lcpo, call);

    final long size = lcpo.buffer.size();
    final List<ByteBuffer> buffers = lcpo.buffer.finish();
    final ByteBuf byteBuf = Unpooled.wrappedBuffer(buffers.toArray(new ByteBuffer[buffers.size()]));

    if (size < MAX_SIZE) {
      final BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
      objects.add(frame);
    } else {
      long remaining = size;
      boolean first = true;
      while (remaining > 0) {
        WebSocketFrame frame;
        if (remaining > MAX_SIZE) {
          if (first) {
            final ByteBuf slice = byteBuf.copy((int) (size - remaining), (int) MAX_SIZE);
            frame = new BinaryWebSocketFrame(false, 0, slice);
            first = false;
          } else {
            final ByteBuf slice = byteBuf.copy((int) (size - remaining), (int) MAX_SIZE);
            frame = new ContinuationWebSocketFrame(false, 0, slice);
          }
          remaining -= MAX_SIZE;
        } else {
          final ByteBuf slice = byteBuf.copy((int) (size - remaining), (int) remaining);
          frame = new ContinuationWebSocketFrame(true, 0, slice);
          remaining = 0;
        }
        objects.add(frame);
      }
    }
  }

  public WebSocketClientHandshaker getHandShaker() {
    return handShaker;
  }
}
