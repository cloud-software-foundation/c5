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

import c5db.client.generated.Call;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.protostuff.ByteBufferInput;

import java.util.List;


/**
 * A specialized Protostuff decoder used to deserialize Protostuff from a WebSocketStream and map them to a Call
 * object.
 */
public class WebsocketProtostuffDecoder extends WebSocketServerProtocolHandler {

  public WebsocketProtostuffDecoder(String websocketPath) {
    super(websocketPath);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, WebSocketFrame frame, List<Object> out) throws Exception {
    if (frame instanceof BinaryWebSocketFrame) {
      final ByteBufferInput input = new ByteBufferInput(frame.content().nioBuffer(), false);
      final Call newMsg = Call.getSchema().newMessage();
      Call.getSchema().mergeFrom(input, newMsg);
      out.add(newMsg);
    } else {
      super.decode(ctx, frame, out);
    }
  }
}
