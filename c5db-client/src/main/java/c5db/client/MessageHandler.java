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

package c5db.client;

import c5db.client.generated.Call;
import c5db.client.generated.Response;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;

public interface MessageHandler extends ChannelHandler, ChannelInboundHandler {

  ListenableFuture<Response> call(Call request, Channel channel);

  ListenableFuture<Response> buffer(Call request, Channel channel);

  ListenableFuture<Long> callScan(Call request, Channel channel);
}
