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

package c5db.tablet;

import c5db.interfaces.C5Server;
import c5db.interfaces.ReplicationModule;
import c5db.tablet.tabletCreationBehaviors.StartableTabletBehavior;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.nio.file.Path;
import java.util.List;

/**
 * Essentially the constructor parameters for a tablet, as an interface. This exists primarily
 * to provide testability for those who manage the lifecycle of Tablets.
 * <p>
 * In production code, one can just type Tablet::new !!
 */
public interface TabletFactory {
  c5db.interfaces.tablet.Tablet create(
      C5Server server,
      HRegionInfo regionInfo,
      HTableDescriptor tableDescriptor,
      List<Long> peers,
      Path basePath,
      Configuration legacyConf,
      ReplicationModule replicationModule,
      Region.Creator regionCreator,
      StartableTabletBehavior userTabletLeaderBehavior);
}
