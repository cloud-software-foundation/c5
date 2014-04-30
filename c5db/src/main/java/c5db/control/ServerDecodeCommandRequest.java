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
import io.netty.handler.codec.MessageToMessageDecoder;
import io.protostuff.ByteBufferInput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * TODO document me here
 */
public class ServerDecodeCommandRequest extends MessageToMessageDecoder<ControlWireMessage> {
  @Override
  protected void decode(ChannelHandlerContext ctx, ControlWireMessage msg, List<Object> out) throws Exception {
    Class<?> protostuffClass = Class.forName(msg.getSubType());
    Schema<Message<?>> protostuffSchemaInstance = (Schema<Message<?>>) protostuffClass.newInstance();

    Message<?> resultingMessage = protostuffSchemaInstance.newMessage();

    ByteBuffer fromMsg = msg.getSubMessage();
    ByteBufferInput input = new ByteBufferInput(fromMsg, false);

    protostuffSchemaInstance.mergeFrom(input, resultingMessage);
    out.add(
        new CommandRpcRequest(msg.getReceipientNodeId(), resultingMessage)
    );
  }
}
