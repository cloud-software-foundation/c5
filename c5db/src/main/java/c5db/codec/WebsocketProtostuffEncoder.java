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
