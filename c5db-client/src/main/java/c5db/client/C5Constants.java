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
package c5db.client;

/**
 * A class to abstract all of the magic numbers for our client
 */
public final class C5Constants {
  public static final int DEFAULT_INIT_SCAN = 100;
  public static final int MAX_REQUEST_SIZE = 1000000;
  public static final int MAX_CACHE_SZ = MAX_REQUEST_SIZE * 2;
  public static final int TEST_PORT = 31337;
  public static final long TIMEOUT = 10000;
  public static final int MAX_CONTENT_LENGTH_HTTP_AGG = 8192;
  public static final int MAX_RESPONSE_SIZE = Integer.MAX_VALUE;
  public static final int IN_FLIGHT_CALLS = 100000;

  private C5Constants() {
    throw new UnsupportedOperationException();
  }
}
