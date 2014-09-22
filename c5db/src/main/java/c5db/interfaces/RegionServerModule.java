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

package c5db.interfaces;

import c5db.messages.generated.ModuleType;

/**
 * Provides remote network services for RPC clients (via http/s) to interact with the database.
 * <p/>
 * The regionserver module may also be responsible for the forwarding client (To be
 * implemented).
 */
@DependsOn(TabletModule.class)
@ModuleTypeBinding(ModuleType.RegionServer)
public interface RegionServerModule extends C5Module {


}
