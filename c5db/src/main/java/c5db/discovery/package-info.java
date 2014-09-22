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

package c5db.discovery;
/**
 The discovery module is responsible for discovering peers in the cluster. It also
 provides the mapping from node/peer id to physical network address (IP/IPv6).
 <p>
 The discovery module can accomplish it's job via multiple methods.
 These might be:
 <ul>
 <li>UDP broadcast/multicast</li>
 <li>DNS discovery</li>
 <li>static configuration file</li>
 </ul>
 <p>
 Currently only UDP discovery is implemented.  This is embodied in the
 {@link c5db.discovery.BeaconService} class.  This class implements the
 {@link c5db.interfaces.DiscoveryModule} interface - but in the future it probably
 shouldn't.
 <p>
 Future work includes:
 <ul>
 <li>Detecting the last time a node was reachable.</li>
 <li>Taking availability reports from other modules (most likely the
 {@link c5db.interfaces.ReplicationModule}</li>
 <li>Something else?</li>
 </ul>
 */