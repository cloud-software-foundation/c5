/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.flease;

/**
 * A way to get dynamically/statically changing information.
 */
public interface InformationInterface {
    /**
     * A facade over 'local time' stuff.  Allows hooks for testing.
     * @return
     */
    public long currentTimeMillis();

    /**
     * Configuration value - time until the lease expires.  Used to
     * create new lease times by adding this returned value to the
     * value returned by getCurrentTimeMillis() above.
     *
     * AKA t-max in Algorithm 4/et al
     * @return
     */
    public long getLeaseLength();
}
