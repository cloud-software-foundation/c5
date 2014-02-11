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
package c5db.client.codec;

import c5db.client.generated.ClientProtos;
import com.google.protobuf.ZeroCopyLiteralByteString;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;


import java.util.List;

public class WebsocketProtostuffDecoder extends MessageToMessageDecoder<BinaryWebSocketFrame> {


  @Override
  protected void decode(ChannelHandlerContext channelHandlerContext,
                        BinaryWebSocketFrame binaryWebSocketFrame,
                        List<Object> objects) throws Exception {


    objects.add(ClientProtos.Response.parseFrom(
        ZeroCopyLiteralByteString.copyFrom(
            binaryWebSocketFrame.content().nioBuffer())));

  }
}
