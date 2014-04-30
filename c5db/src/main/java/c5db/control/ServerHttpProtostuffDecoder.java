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
