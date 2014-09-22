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

/**
 C5 intends to have a strict interface separation between different internal modules.  This helps
 us be explicit about how different internal parts depend on each other.  It also has the
 advantage of being easy to audit, and cross-package dependencies can be strictly enforced.

 The 'root' interface is {@link c5db.interfaces.C5Module} which describes what a module is,
 and the basic interfaces it needs to support.

 Additionally there is a {@link c5db.interfaces.C5Server} interface which describes the
 central binding service that all other services may depend on for "global" resources, config,
 cross module discovery/binding at runtime and any other mechanisms in which a module
 might have to do server wide things.
 */