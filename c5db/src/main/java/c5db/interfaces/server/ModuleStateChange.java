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
package c5db.interfaces.server;

import c5db.interfaces.C5Module;
import com.google.common.util.concurrent.Service;

/**
 * Notification object about when a module has a state change.
 */
public class ModuleStateChange {
  public final C5Module module;
  public final Service.State state;

  @Override
  public String toString() {
    return "ModuleStateChange{" +
        "module=" + module +
        ", state=" + state +
        '}';
  }

  public ModuleStateChange(C5Module module, Service.State state) {
    this.module = module;
    this.state = state;
  }
}
