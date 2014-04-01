
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