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
package c5db.regionserver;

import org.jetlang.channels.Channel;

import java.util.concurrent.ConcurrentHashMap;

/**
 * A JVM wide mapping from scannerIds to netty channels so that a user can access the scanner channels.
 */
public enum ScannerManager {
  INSTANCE;

  private ConcurrentHashMap<Long, Channel<Integer>> scannerMap = new ConcurrentHashMap<>();

  ScannerManager() {  }

  public Channel<Integer> getChannel(long scannerId) {
    return scannerMap.get(scannerId);
  }

  public void addChannel(long scannerId, Channel<Integer> channel) {
    scannerMap.put(scannerId, channel);
  }
}
