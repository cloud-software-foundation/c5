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

package c5db.testing;

import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;

/**
 * A Hamcrest matcher for comparing byte arrays.
 */
public class BytesEqualMatcher extends TypeSafeMatcher<byte[]> {
  private final byte[] precond;

  public BytesEqualMatcher(byte[] precond) {
    this.precond = precond;
  }

  public static String printFirstBytes(byte[] bytes, int firstXBytes) {
    String rep = Bytes.toStringBinary(bytes, 0, Math.min(firstXBytes, bytes.length));
    if (bytes.length > firstXBytes) {
      return rep + "...(" + (bytes.length - firstXBytes) + " more)";
    }
    return rep;
  }

  @Override
  protected boolean matchesSafely(byte[] item) {
    return Arrays.equals(item, precond);
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("a byte array")
        .appendText(printFirstBytes(precond, 100));
  }
}
