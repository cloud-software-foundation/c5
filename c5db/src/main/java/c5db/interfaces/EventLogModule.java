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

import c5db.eventLogging.generated.EventLogEntry;
import c5db.messages.generated.ModuleType;
import org.jetlang.channels.Subscriber;

/**
 * The Event log module provides a system-wide service for publishing high value events.
 * This module then can go on to do whatever it wishes with the data, the first implementations
 * might broadcast them on UDP to facilitate remote observation and debugging.
 */
@ModuleTypeBinding(ModuleType.EventLog)
public interface EventLogModule extends C5Module {

  Subscriber<EventLogEntry> eventLogChannel();
}
