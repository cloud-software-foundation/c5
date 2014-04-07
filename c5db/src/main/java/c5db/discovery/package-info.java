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