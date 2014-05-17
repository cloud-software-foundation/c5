/*
 * Copyright (C) 2013  Ohm Data
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
