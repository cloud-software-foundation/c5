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
package c5db.client.codec;

import c5db.client.C5Constants;
import c5db.client.generated.Call;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.protostuff.LowCopyProtobufOutput;

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

    if (size < MAX_SIZE) {
      ByteBuf byteBuf = channelHandlerContext.alloc().buffer((int) size);
      lcpo.buffer.finish().stream().forEach(byteBuf::writeBytes);
      final BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
      objects.add(frame);
    } else {
      long remaining = size;
      boolean first = true;
      ByteBuf byteBuf = channelHandlerContext.alloc().buffer((int) size);
      lcpo.buffer.finish().stream().forEach(byteBuf::writeBytes);

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

  public WebSocketClientHandshaker getHandShaker() {
    return handShaker;
  }
}