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
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Client - Encodes Protostuff-based objects into FullHttpRequest messages.
 */
public class ClientHttpProtostuffEncoder extends MessageToMessageEncoder<Message> {
  @Override
  protected void encode(ChannelHandlerContext ctx, Message msg, List<Object> out) throws Exception {
    Class<?> messageType = msg.getClass();

    LowCopyProtobufOutput outputSerializer = new LowCopyProtobufOutput();
    msg.cachedSchema().writeTo(outputSerializer, msg);
    List<ByteBuffer> serializedBuffers = outputSerializer.buffer.finish();
    ByteBuf requestContent = Unpooled.wrappedBuffer(serializedBuffers.toArray(new ByteBuffer[]{}));

    DefaultFullHttpRequest httpRequest = new DefaultFullHttpRequest(
        HttpVersion.HTTP_1_0,
        HttpMethod.POST,
        "foo",
        requestContent);

    httpRequest.headers().set(HttpProtostuffConstants.PROTOSTUFF_HEADER_NAME,
        messageType.getName());
    httpRequest.headers().set(HttpHeaders.Names.CONTENT_TYPE,
        "application/octet-stream");
    httpRequest.headers().set(HttpHeaders.Names.CONTENT_LENGTH,
        requestContent.readableBytes());

    out.add(httpRequest);
  }
}
