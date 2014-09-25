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
