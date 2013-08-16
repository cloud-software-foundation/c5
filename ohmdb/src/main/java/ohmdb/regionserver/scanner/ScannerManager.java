package ohmdb.regionserver.scanner;

import org.jetlang.channels.Channel;

import java.util.concurrent.ConcurrentHashMap;

public enum ScannerManager {
  INSTANCE;

  private ConcurrentHashMap<Long, Channel<Integer>> scannerMap =
      new ConcurrentHashMap<>();


  ScannerManager() {
  }

  public Channel<Integer> getChannel(long scannerId) {
    return scannerMap.get(scannerId);
  }

  public void addChannel(long scannerId, Channel<Integer> channel) {
    scannerMap.put(scannerId, channel);
  }
}
