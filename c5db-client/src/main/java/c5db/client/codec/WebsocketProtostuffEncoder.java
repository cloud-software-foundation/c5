/*
 * Copyright (C) 2013  Ohm Data
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;

import java.util.List;

public class WebsocketProtostuffEncoder extends MessageToMessageEncoder<ClientProtos.Call> {
  @Override
  protected void encode(ChannelHandlerContext channelHandlerContext,
                        ClientProtos.Call call,
                        List<Object> objects) throws Exception {
    ByteBuf byteBuf = Unpooled.copiedBuffer(call.toByteArray());
    BinaryWebSocketFrame frame = new BinaryWebSocketFrame(byteBuf);
    objects.add(frame);
  }

}
