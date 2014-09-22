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
package c5db.client.scanner;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.channel.Channel;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public enum ClientScannerManager {
  INSTANCE;

  private final ConcurrentHashMap<Long, SettableFuture<ClientScanner>> scannerMap = new ConcurrentHashMap<>();

  public ClientScanner createAndGet(Channel channel, long scannerId, long commandId) throws IOException {
    if (hasScanner(scannerId)) {
      throw new IOException("Scanner already created");
    }

    final ClientScanner scanner = new ClientScanner(channel, scannerId, commandId);
    SettableFuture<ClientScanner> clientScannerSettableFuture = SettableFuture.create();
    clientScannerSettableFuture.set(scanner);
    scannerMap.put(scannerId, clientScannerSettableFuture);
    return scanner;
  }

  public ListenableFuture<ClientScanner> get(long scannerId) throws ExecutionException, InterruptedException {
    return scannerMap.get(scannerId);
  }

  public boolean hasScanner(long scannerId) {
    return scannerMap.containsKey(scannerId);
  }
}
