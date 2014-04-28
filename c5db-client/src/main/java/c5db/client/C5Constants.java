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
package c5db.client;

/**
 * A class to abstract all of the magic numbers for our client
 */
public final class C5Constants {
  private C5Constants() {
    throw new UnsupportedOperationException();
  }

  public static final int DEFAULT_INIT_SCAN = 100;
  public static final int MAX_REQUEST_SIZE = 1000000;
  public static final int MAX_CACHE_SZ = MAX_REQUEST_SIZE * 2;
  public static final int TEST_PORT = 31337;
  public static final long TIMEOUT = 10000;
  public static final int MAX_CONTENT_LENGTH_HTTP_AGG = 8192;
  public static final int MAX_RESPONSE_SIZE = Integer.MAX_VALUE;
}
