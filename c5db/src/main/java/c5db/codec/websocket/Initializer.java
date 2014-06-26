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
package c5db.codec.websocket;

import c5db.C5ServerConstants;
import c5db.client.generated.Call;
import c5db.client.generated.Response;
import c5db.codec.protostuff.Decoder;
import c5db.codec.protostuff.LowCopyProtobufOutputEncoder;
import c5db.regionserver.RegionServerHandler;
import c5db.regionserver.RegionServerService;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class Initializer extends ChannelInitializer<SocketChannel> {

  private final RegionServerService regionServerService;

  public Initializer(RegionServerService regionServerService) {
    this.regionServerService = regionServerService;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline p = ch.pipeline();
    p.addLast("http-server-codec", new HttpServerCodec());
    p.addLast("http-agg", new HttpObjectAggregator(C5ServerConstants.MAX_CALL_SIZE));

    p.addLast("websocket-decoder", new c5db.codec.websocket.Decoder("/websocket"));
    p.addLast("protostuff-decoder", new Decoder<>(Call.getSchema()));

    p.addLast("websocket-encoder", new Encoder());
    p.addLast("protostuff-encoder", new LowCopyProtobufOutputEncoder<Response>());

    p.addLast("handler", new RegionServerHandler(regionServerService));
  }
}

