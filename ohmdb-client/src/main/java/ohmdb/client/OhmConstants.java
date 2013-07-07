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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */

package ohmdb.client;

public class OhmConstants {
  public static final int DEFAULT_INIT_SCAN = 100;
  public static final int MAX_REQUEST_SIZE = 1000000;
  public static final long FLUSH_PERIOD = 30000;
  public static final int AMOUNT_OF_FLUSH_PER_COMPACT = 10;
  public static final long SCANNER_TIMEOUT_MS = 3000;
  public static final int MSG_SIZE = 100;
  public static int TEST_PORT = 8080;
  public static int MAX_CACHE_SZ = MAX_REQUEST_SIZE * 2;
}