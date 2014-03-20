package c5db.client;

import io.netty.channel.Channel;
import org.mortbay.log.Log;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;


/***
 * A helper singleton to store all of the regions that the client jvm is connected to.
 */
public enum RegionChannelMap {
  INSTANCE;

  private static final ConcurrentHashMap<String, Channel> REGION_CHANNEL_MAP = new ConcurrentHashMap<>();

  public boolean containsKey(String key) {
    return REGION_CHANNEL_MAP.containsKey(key);
  }

  public void put(String key, Channel value) {
    REGION_CHANNEL_MAP.put(key, value);
  }

  public Channel get(String key) {
    return REGION_CHANNEL_MAP.get(key);
  }

  public void clear() {
    REGION_CHANNEL_MAP.clear();
  }

  public Collection<Channel> getValues() {
    return REGION_CHANNEL_MAP.values();
  }

  public void remove(String hash) {
    Log.warn("remove hash:" + hash);
    REGION_CHANNEL_MAP.remove(hash);
  }
}
