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
