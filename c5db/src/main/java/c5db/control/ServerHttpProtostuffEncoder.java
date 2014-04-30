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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Server side, http encoder.
 */
public class ServerHttpProtostuffEncoder extends MessageToMessageEncoder<Message> {
  @Override
  protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
    Class<?> messageType = msg.getClass();

    LowCopyProtobufOutput outputSerializer = new LowCopyProtobufOutput();
    msg.cachedSchema().writeTo(outputSerializer, msg);
    List<ByteBuffer> serializedBuffers = outputSerializer.buffer.finish();
    ByteBuf replyContent = Unpooled.wrappedBuffer(serializedBuffers.toArray(new ByteBuffer[]{}));

    DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(
        HttpVersion.HTTP_1_0, HttpResponseStatus.OK,
        replyContent);
    httpResponse.headers().set(HttpProtostuffConstants.PROTOSTUFF_HEADER_NAME,
        messageType.getName());
    httpResponse.headers().set(HttpHeaders.Names.CONTENT_TYPE,
        "application/octet-stream");

    out.add(httpResponse);

  }
}
