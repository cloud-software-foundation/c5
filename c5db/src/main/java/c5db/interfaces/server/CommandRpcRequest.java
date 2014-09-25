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

package c5db.interfaces.server;

import io.protostuff.Message;

/**
 * Send a command.
 */
public class CommandRpcRequest<T extends Message> {

  public final long receipientNodeId;
  public final T message;

  public CommandRpcRequest(long receipientNodeId, T message) {
    this.receipientNodeId = receipientNodeId;
    this.message = message;
  }

  @Override
  public String toString() {
    return "CommandRpcRequest{" +
        "receipientNodeId=" + receipientNodeId +
        ", message=" + message +
        '}';
  }
}
