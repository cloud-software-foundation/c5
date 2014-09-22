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
