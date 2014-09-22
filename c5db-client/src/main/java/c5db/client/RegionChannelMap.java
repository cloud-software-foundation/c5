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

import io.netty.channel.Channel;
import org.mortbay.log.Log;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;


/**
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
