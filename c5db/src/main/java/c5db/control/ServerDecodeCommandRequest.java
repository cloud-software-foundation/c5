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
