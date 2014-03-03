package c5db.client;

import io.netty.channel.Channel;
import org.mortbay.log.Log;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;


public enum RegionChannelMap {
  INSTANCE;

  private static ConcurrentHashMap<String, Channel> regionChannelMap = new ConcurrentHashMap<>();

  public boolean containsKey(String key) {
    return regionChannelMap.containsKey(key);
  }

  public void put(String key, Channel value) {
    regionChannelMap.put(key, value);
  }

  public Channel get(String key) {
    return regionChannelMap.get(key);
  }

  public void clear() {
    regionChannelMap.clear();
  }

  public Collection<Channel> getValues() {
    return regionChannelMap.values();
  }

  public void remove(String hash) {
    Log.warn("remove hash:" + hash);
    regionChannelMap.remove(hash);
  }
}
