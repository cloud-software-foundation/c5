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

package c5db.codec.protostuff;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.protostuff.LowCopyProtobufOutput;
import io.protostuff.Message;
import io.protostuff.Schema;

import java.util.List;

public class LowCopyProtobufOutputEncoder<T extends Message<T>> extends MessageToMessageEncoder<Message<T>> {
  @Override
  protected void encode(ChannelHandlerContext ctx, Message<T> msg, List<Object> out) throws Exception {
    Schema<T> schema = msg.cachedSchema();
    LowCopyProtobufOutput lcpo = new LowCopyProtobufOutput();
    schema.writeTo(lcpo, (T) msg);
    out.add(lcpo);
  }
}
