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

import c5db.client.generated.Response;
import com.dyuproject.protostuff.LowCopyProtobufOutput;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;

import java.nio.ByteBuffer;
import java.util.List;

public class WebsocketProtostuffEncoder extends MessageToMessageEncoder<Response> {
  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        Response response,
                        List<Object> objects) throws Exception {
    LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput();
    Response.getSchema().writeTo(lcpo, response);
    List<ByteBuffer> buffers = lcpo.buffer.finish();
    ByteBuf byteBuf = Unpooled.wrappedBuffer(buffers.toArray(new ByteBuffer[buffers.size()]));
    BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
    objects.add(frame);
  }
}
