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

import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.ControlWireMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * TODO document me here
 */
public class ClientEncodeCommandRequest extends
    MessageToMessageEncoder<CommandRpcRequest<? extends Message>> {

  @Override
  protected void encode(ChannelHandlerContext ctx, CommandRpcRequest<? extends Message> msg, List<Object> out) throws Exception {

    LowCopyProtobufOutput outputSerializer = new LowCopyProtobufOutput();
    msg.message.cachedSchema().writeTo(outputSerializer, msg.message);
    List<ByteBuffer> ser0 = outputSerializer.buffer.finish();
    int size = (int) outputSerializer.buffer.size();

    ByteBuffer ser = ByteBuffer.allocate(size);
    for (ByteBuffer b : ser0) {
      ser.put(b);
    }

    ser.flip();

    ControlWireMessage wireMsg = new ControlWireMessage(msg.receipientNodeId,
        msg.message.getClass().getName(),
        ser);

    out.add(wireMsg);
  }
}
