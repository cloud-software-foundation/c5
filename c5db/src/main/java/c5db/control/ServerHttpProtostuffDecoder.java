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
package c5db.control;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.protostuff.ByteBufferInput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Server Side FullHttpRequest decoder.
 */
public class ServerHttpProtostuffDecoder extends MessageToMessageDecoder<FullHttpRequest> {
  @Override
  protected void decode(ChannelHandlerContext ctx, FullHttpRequest request, List<Object> out) throws Exception {
    String protostuffJavaMessageType = request.headers().get(HttpProtostuffConstants.PROTOSTUFF_HEADER_NAME);

    Class<?> protostuffClass = Class.forName(protostuffJavaMessageType);
    Schema<Message<?>> protostuffSchemaInstance = (Schema<Message<?>>) protostuffClass.newInstance();

    Message<?> resultingMessage = protostuffSchemaInstance.newMessage();

    ByteBuffer content = request.content().nioBuffer();
    ByteBufferInput protostuffDecoder = new ByteBufferInput(content, false);

    protostuffSchemaInstance.mergeFrom(protostuffDecoder, resultingMessage);

    out.add(resultingMessage);
  }
}
